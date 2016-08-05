// -----------------------------------------------------------------------
// <copyright file="ContinuousFuturesBroker.cs" company="">
// Copyright 2013 Alexander Soffronow Pagonidis
// </copyright>
// -----------------------------------------------------------------------

// This class serves two primary functions: construct continuous futures data,
// and find what the current "front" contract for a continuous future instrument is.
//
// How the data works:
// Receive a request for data in RequestHistoricalData().
// In there, figure out which actual futures contracts we need, and request their data.
// Keep track of that stuff in _requestIDs.
// When all the data has arrived, figure out how to stich it together in CalcContFutData()
// Finally send it out using the HistoricalDataArrived event.
//
// How finding the front contract works:
// Receive request on RequestFrontContract() and return a unique ID identifying this request.
// If it's a time-based switch over, just calculate it on the spot.
// Otherwise we have to grab the data and do the calculations in CalcContFutData() which returns
// the final contract used.
// The result is findally returned through the FoundFrontContract event.


using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Timers;

using NLog;

using QDMS;
using QDMS.Annotations;

using Timer = System.Timers.Timer;

#pragma warning disable 67

// ReSharper disable once CheckNamespace
namespace QDMSServer
{
    public class ContinuousFuturesBroker : IContinuousFuturesBroker
    {
        #region Variables
        /// <summary>
        ///     Keeps track of how many historical data requests remain until we can calculate the continuous prices
        ///     Key: request ID, Value: number of requests outstanding
        /// </summary>
        private readonly Dictionary<int, int> _requestCounts;
        /// <summary>
        ///     This keeps track of which futures contract requests belong to which continuous future request
        ///     Key: contract request ID, Value: continuous future request ID
        /// </summary>
        private readonly Dictionary<int, int> _histReqIDMap;
        /// <summary>
        ///     Key: request ID, Value: the list of contracts used to fulfill this request
        /// </summary>
        private readonly Dictionary<int, List<Instrument>> _contracts;
        /// <summary>
        ///     Keeps track of the requests. Key: request ID, Value: the request
        /// </summary>
        private readonly Dictionary<int, HistoricalDataRequest> _requests;
        /// <summary>
        ///     Front contract requests that need data to be downloaded, receive a HistoricalDataRequest
        ///     that is held in _requests. The same AssignedID also corresponds to a FrontContractRequest held here.
        /// </summary>
        private readonly Dictionary<int, FrontContractRequest> _frontContractRequestMap;
        /// <summary>
        ///     keeps track of whether requests are for data, or for the front contract. True = data.
        /// </summary>
        private readonly Dictionary<int, bool> _requestTypes;
        /// <summary>
        ///     Holds requests for the front contract of a CF, before they get processed
        /// </summary>
        private readonly BlockingCollection<FrontContractRequest> _frontContractRequests;
        /// <summary>
        ///     This dictionary uses instrument IDs as keys, and holds the data that we use to construct our futures
        ///     Key: a KVP where key: the instrument ID, and value: the data frequency
        ///     Value: data
        /// </summary>
        private readonly Dictionary<KeyValuePair<int, BarSize>, List<OHLCBar>> _data;
        /// <summary>
        ///     Some times there will be multiple requests for data being filled concurrently.
        ///     And sometimes they will be for the same instrument. Thus we can't safely delete data from _data,
        ///     because it might still be needed!
        ///     So here we keep track of the number of requests that are going to use the data...
        ///     and use that number to then finally free up the memory when all related requests are completed
        ///     Key: a KVP where key: the instrument ID, and value: the data frequency
        ///     Value: data
        /// </summary>
        private readonly Dictionary<KeyValuePair<int, BarSize>, int> _dataUsesPending;

        private readonly Logger _logger = LogManager.GetCurrentClassLogger();

        private readonly IInstrumentSource _instrumentMgr;
        private readonly Timer _reconnectTimer;

        private readonly object _reqCountLock = new object();
        private readonly object _requestsLock = new object();
        private readonly object _dataUsesLock = new object();
        private readonly object _dataLock = new object();
        private readonly object _frontContractReturnLock = new object();

        private IDataClient _client;
        /// <summary>
        ///     Used to give unique IDs to front contract requests
        /// </summary>
        private int _lastFrontDontractRequestID;
        #endregion

        #region IDisposable implementation
        public void Dispose()
        {
            _reconnectTimer?.Dispose();

            if (_client != null)
            {
                _client.Dispose();
                _client = null;
            }

            _frontContractRequests?.Dispose();
        }
        #endregion

        #region IHistoricalDataSource implementation
        /// <summary>
        ///     Whether the connection to the data source is up or not.
        /// </summary>
        public bool Connected => true;

        /// <summary>
        ///     The name of the data source.
        /// </summary>
        public string Name { get; }

        /// <summary>
        ///     Connect to the data source.
        /// </summary>
        public void Connect() {}

        /// <summary>
        ///     Disconnect from the data source.
        /// </summary>
        public void Disconnect() { }

        /// <summary>
        ///     Make a request for historical continuous futures data.
        ///     The data is returned through the HistoricalDataArrived event.
        /// </summary>
        public void RequestHistoricalData(HistoricalDataRequest request)
        {
            _logger.Info(
                $"CFB: Received historical data request: {request.Instrument.Symbol} @ {request.Frequency} ({request.Instrument.Datasource.Name}) from {request.StartingDate} to {request.EndingDate} - ID: {request.AssignedID}");
            // Add it to the collection of requests so we can access it later
            lock (_requestsLock) {
                _requests.Add(request.AssignedID, request);
            }

            _requestTypes.Add(request.AssignedID, true);
            // Find what contracts we need
            var reqs = GetRequiredRequests(request);
            // If nothing is found, return right now with no data
            if (reqs.Count == 0) {
                RaiseEvent(HistoricalDataArrived, this, new HistoricalDataEventArgs(request, new List<OHLCBar>()));

                return;
            }
            // Send out the requests
            foreach (var req in reqs) {
                lock (_dataUsesLock) {
                    var kvp = new KeyValuePair<int, BarSize>(req.Instrument.ID.Value, req.Frequency);

                    if (_dataUsesPending.ContainsKey(kvp)) {
                        _dataUsesPending[kvp]++;
                    }
                    else {
                        _dataUsesPending.Add(kvp, 1);
                    }
                }

                var requestID = _client.RequestHistoricalData(req);

                _histReqIDMap.Add(requestID, request.AssignedID);
            }
        }

        public event EventHandler<HistoricalDataEventArgs> HistoricalDataArrived;

        public event EventHandler<ErrorArgs> Error;

        public event EventHandler<DataSourceDisconnectEventArgs> Disconnected;
        #endregion

        #region  IContinuousFuturesBroker implementation
        /// <summary>
        ///     Finds the currently active futures contract for a continuous futures instrument.
        ///     The contract is returned asynchronously through the FoundFrontContract event.
        /// </summary>
        /// <returns>Returns an ID uniquely identifying this request.</returns>
        public int RequestFrontContract(Instrument cfInstrument, DateTime? date = null)
        {
            if (!cfInstrument.IsContinuousFuture) {
                throw new Exception("Not a continuous future instrument.");
            }

            var id = Interlocked.Increment(ref _lastFrontDontractRequestID);

            var req = new FrontContractRequest
            {
                ID = id,
                Instrument = cfInstrument,
                Date = date
            };

            ProcessFrontContractRequest(req);

            return id;
        }

        public event EventHandler<FoundFrontContractEventArgs> FoundFrontContract;
        #endregion

        public ContinuousFuturesBroker(IDataClient client, IInstrumentSource instrumentMgr, bool connectImmediately = true)
        {
            if (client == null) {
                throw new ArgumentNullException(nameof(client));
            }

            _client = client;

            _instrumentMgr = instrumentMgr;

            _client.HistoricalDataReceived += ClientHistoricalDataReceived;
            _client.Error += ClientError;

            if (connectImmediately) {
                _client.Connect();
            }


            _data = new Dictionary<KeyValuePair<int, BarSize>, List<OHLCBar>>();
            _contracts = new Dictionary<int, List<Instrument>>();
            _requestCounts = new Dictionary<int, int>();
            _requests = new Dictionary<int, HistoricalDataRequest>();
            _histReqIDMap = new Dictionary<int, int>();
            _frontContractRequests = new BlockingCollection<FrontContractRequest>();
            _requestTypes = new Dictionary<int, bool>();
            _frontContractRequestMap = new Dictionary<int, FrontContractRequest>();
            _dataUsesPending = new Dictionary<KeyValuePair<int, BarSize>, int>();

            _reconnectTimer = new Timer(1000);
            _reconnectTimer.Elapsed += ReconnectTimerElapsed;
            _reconnectTimer.Start();

            Name = "ContinuousFutures";
        }

        #region Event handlers
        private void ReconnectTimerElapsed(object sender, ElapsedEventArgs e)
        {
            if (!_client.Connected) {
                _reconnectTimer.Stop();

                _logger.Info("CFB: trying to reconnect.");

                try {
                    _client.Connect();
                }
                catch (Exception ex) {
                    _logger.Error(ex, "CFB error when trying to connect.");
                }
            }
        }

        /// <summary>
        ///     Historical data has arrived.
        ///     Add it to our data store, then check if all requests have been
        ///     fulfilled for a particular continuous futures request. If they
        ///     have, go do the calculations.
        /// </summary>
        private void ClientHistoricalDataReceived(object sender, HistoricalDataEventArgs e)
        {
            if (e.Request.Instrument.ID == null) return;
            var id = e.Request.Instrument.ID.Value;
            var kvpID = new KeyValuePair<int, BarSize>(id, e.Request.Frequency);

            lock (_data) {
                if (_data.ContainsKey(kvpID)) {
                    // We already have data on this instrument ID/Frequency combo.
                    // Add the arrived data, then discard any doubles, then order.
                    _data[kvpID].AddRange(e.Data);
                    _data[kvpID] = _data[kvpID].Distinct((x, y) => x.DT == y.DT).ToList();
                    _data[kvpID] = _data[kvpID].OrderBy(x => x.DT).ToList();
                }
                else {
                    // We have nothing on this instrument ID/Frequency combo.
                    // Just add a new entry in the dictionary.
                    _data.Add(kvpID, e.Data);
                }
            }

            // Here we check if all necessary requests have arrived. If they have, do some work.
            lock (_reqCountLock) {
                // Get the request id of the continuous futures request that caused this contract request
                var cfReqID = _histReqIDMap[e.Request.RequestID];
                _histReqIDMap.Remove(e.Request.RequestID);
                _requestCounts[cfReqID]--;

                if (_requestCounts[cfReqID] == 0) {
                    // We have received all the data we asked for
                    _requestCounts.Remove(e.Request.RequestID);
                    HistoricalDataRequest req;
                    lock (_requestsLock) {
                        req = _requests[cfReqID];
                        _requests.Remove(cfReqID);
                    }

                    if (_requestTypes[cfReqID]) {
                        // This request originates from a CF data request so now we want to generate the continuous prices
                        GetContFutData(req);
                    }
                    else {
                        // This request originates from a front contract request
                        var frontContract = GetContFutData(req, false);
                        var originalReq = _frontContractRequestMap[cfReqID];
                        lock (_frontContractReturnLock) {
                            RaiseEvent(
                                FoundFrontContract,
                                this,
                                new FoundFrontContractEventArgs(originalReq.ID, frontContract, originalReq.Date ?? DateTime.Now));
                        }
                    }

                    _requestTypes.Remove(e.Request.AssignedID);
                }
            }
        }

        private void ClientError(object sender, ErrorArgs e)
        {
            _logger.Error("Continuous futures broker client error: " + e.ErrorMessage);
        }
        #endregion

        /// <summary>
        ///     Taking a historical data request for a continuous futures instrument,
        ///     it returns a list of requests for the underlying contracts that are needed to fulfill it.
        /// </summary>
        private List<HistoricalDataRequest> GetRequiredRequests(HistoricalDataRequest request)
        {
            var requests = new List<HistoricalDataRequest>();
            var cf = request.Instrument.ContinuousFuture;
            var searchInstrument = new Instrument
            {
                UnderlyingSymbol = request.Instrument.ContinuousFuture.UnderlyingSymbol.Symbol,
                Type = InstrumentType.Future,
                DatasourceID = request.Instrument.DatasourceID
            };
            var futures = _instrumentMgr.FindInstruments(search: searchInstrument);

            if (futures == null) {
                _logger.Error($"CFB: Error in GetRequiredRequests, failed to return any contracts to historical request ID: {request.AssignedID}");

                return requests;
            }
            // Remove any continuous futures
            futures = futures.Where(x => !x.IsContinuousFuture).ToList();
            // Order them by ascending expiration date
            futures = futures.OrderBy(x => x.Expiration).ToList();
            // Filter the futures months, we may not want all of them.
            for (var i = 1;i <= 12;i++) {
                if (cf.MonthIsUsed(i)) {
                    continue;
                }
                // TODO: Why it is here?
                var i1 = i;

                futures = futures.Where(x => x.Expiration.HasValue && x.Expiration.Value.Month != i1).ToList();
            }
            // Nothing found, return with empty hands
            if (futures.Count == 0) {
                return requests;
            }
            // The first contract we need is the first one expiring before the start of the request period
            var expiringBeforeStart = futures
                .Where(x => x.Expiration != null && x.Expiration.Value < request.StartingDate)
                .Select(x => x.Expiration.Value).ToList();
            var firstExpiration =
                expiringBeforeStart.Count > 0
                    ? expiringBeforeStart.Max()
                    : futures.Select(x => x.Expiration.Value).Min();

            futures = futures.Where(x => x.Expiration != null && x.Expiration.Value >= firstExpiration).ToList();
            // I think the last contract we need is the one that is N months after the second contract that expires after the request period end
            // where N is the number of months away from the front contract that the CF uses
            var firstExpAfterEnd = futures.Where(x => x.Expiration > request.EndingDate).ElementAtOrDefault(1);

            if (firstExpAfterEnd != null) {
                var limitDate = firstExpAfterEnd.Expiration.Value.AddMonths(request.Instrument.ContinuousFuture.Month - 1);

                futures = futures.Where(
                    x => x.Expiration.Value.Year < limitDate.Year ||
                         (x.Expiration.Value.Year == limitDate.Year && x.Expiration.Value.Month <= limitDate.Month)).ToList();
            }
            // Make sure each month's contract is allowed only once, even if there are multiple copies in the db
            // Sometimes you might get two versions of the same contract with 1 day difference in expiration date
            // so this step is necessary to clean that up
            futures = futures.Distinct(x => x.Expiration.Value.ToString("yyyyMM").GetHashCode()).ToList();
            // Save the number of requests we're gonna make
            lock (_reqCountLock) {
                _requestCounts.Add(request.AssignedID, futures.Count);
            }
            // Save the contracts used, we need them later
            _contracts.Add(request.AssignedID, futures);

            _logger.Info($"CFB: fulfilling historical request ID {request.AssignedID}, requested data on contracts: {string.Join(", ", futures.Select(x => x.Symbol))}");

            Instrument prevInst = null;
            // Request the data for all futures left
            foreach (var i in futures) {
                // The question of how much data, exactly, to ask for is complicated...
                // I'm going with: difference of expiration dates (unless it's the first one, in which case 30 days)
                // plus a month
                // plus the CF selected month
                var daysBack = 30;

                if (prevInst == null) {
                    daysBack += 30;
                }
                else {
                    daysBack += (int) (i.Expiration.Value - prevInst.Expiration.Value).TotalDays;
                }

                daysBack += 30 * cf.Month;

                var endDate = i.Expiration.Value > DateTime.Now.Date ? DateTime.Now.Date : i.Expiration.Value;
                var req = new HistoricalDataRequest(
                    i,
                    request.Frequency,
                    endDate.AddDays(-daysBack),
                    endDate,
                    rthOnly: request.RTHOnly,
                    dataLocation: request.DataLocation == DataLocation.LocalOnly ? DataLocation.LocalOnly : DataLocation.Both);

                requests.Add(req);

                prevInst = i;
            }

            return requests;
        }

        /// <summary>
        /// </summary>
        /// <returns>The last contract used in the construction of this continuous futures instrument.</returns>
        private Instrument GetContFutData(HistoricalDataRequest request, bool raiseDataEvent = true)
        {
            // Copy over the list of contracts that we're gonna be using
            var futures = new List<Instrument>(_contracts[request.AssignedID]);
            // Start by cleaning up the data, it is possible that some of the futures may not have had ANY data returned!
            lock (_dataLock) {
                futures = futures.Where(x => _data[new KeyValuePair<int, BarSize>(x.ID.Value, request.Frequency)].Count > 0).ToList();
            }

            var cf = request.Instrument.ContinuousFuture;
            var frontFuture = futures.FirstOrDefault();
            var backFuture = futures.ElementAt(1);

            if (frontFuture == null) {
                if (raiseDataEvent) {
                    RaiseEvent(HistoricalDataArrived, this, new HistoricalDataEventArgs(request, new List<OHLCBar>()));
                }

                return null;
            }
            // Sometimes the contract will be based on the Xth month
            // This is where we keep track of the actual contract currently being used
            var selectedFuture = futures.ElementAt(cf.Month - 1);
            var lastUsedSelectedFuture = selectedFuture;
            // Final date is the earliest of: the last date of data available, or the request's endingdate
            var lastDateAvailable = new DateTime(1, 1, 1);
            TimeSeries frontData, backData, selectedData;

            lock (_dataLock) {
                frontData = new TimeSeries(_data[new KeyValuePair<int, BarSize>(frontFuture.ID.Value, request.Frequency)]);
                backData = new TimeSeries(_data[new KeyValuePair<int, BarSize>(backFuture.ID.Value, request.Frequency)]);
                selectedData = new TimeSeries(_data[new KeyValuePair<int, BarSize>(selectedFuture.ID.Value, request.Frequency)]);

                lastDateAvailable = _data[new KeyValuePair<int, BarSize>(futures.Last().ID.Value, request.Frequency)].Last().DT;
            }

            var finalDate = request.EndingDate < lastDateAvailable ? request.EndingDate : lastDateAvailable;
            // This is a super dirty hack to make non-time based rollovers actually work.
            // The reason is that the starting point will otherwise be a LONG time before the date we're interested in.
            // And at that time both the front and back futures are really far from expiration.
            // As such volumes can be wonky, and thus result in a rollover far before we ACTUALLY would
            // Want to roll over if we had access to even earlier data.
            var currentDate = frontFuture.Expiration.Value.AddDays(-20 - cf.RolloverDays);

            frontData.AdvanceTo(currentDate);
            backData.AdvanceTo(currentDate);
            selectedData.AdvanceTo(currentDate);

            var cfData = new List<OHLCBar>();
            var calendar = MyUtils.GetCalendarFromCountryCode("US");
            var switchContract = false;
            var counter = 0; // Some rollover rules require multiple consecutive days of greater vol/OI...this keeps track of that
            var frontDailyVolume = new List<long>(); // Keeps track of how much volume has occured in each day
            var frontDailyOpenInterest = new List<int>(); // Keeps track of open interest on a daily basis
            var backDailyVolume = new List<long>();
            var backDailyOpenInterest = new List<int>();
            long frontTodaysVolume = 0, backTodaysVolume = 0;
            // Add the first piece of data we have available, and start looping
            cfData.Add(selectedData[0]);
            // The first time we go from one day to the next we don't want to check for switching conditions
            // Because we need to ensure that we use an entire day's worth of volume data.
            var firstDaySwitchover = true;

            while (currentDate < finalDate) {
                // Keep track of total volume "today"
                if (frontData[0].Volume.HasValue) {
                    frontTodaysVolume += frontData[0].Volume.Value;
                }
                if (backData?[0].Volume != null) {
                    backTodaysVolume += backData[0].Volume.Value;
                }
                if (frontData.CurrentBar > 0 && frontData[0].DT.Day != frontData[1].DT.Day) {
                    if (firstDaySwitchover) {
                        firstDaySwitchover = false;
                        frontTodaysVolume = 0;
                        backTodaysVolume = 0;
                    }

                    frontDailyVolume.Add(frontTodaysVolume);
                    backDailyVolume.Add(backTodaysVolume);

                    if (frontData[0].OpenInterest.HasValue) {
                        frontDailyOpenInterest.Add(frontData[0].OpenInterest.Value);
                    }
                    if (backData?[0].OpenInterest != null) {
                        backDailyOpenInterest.Add(backData[0].OpenInterest.Value);
                    }

                    frontTodaysVolume = 0;
                    backTodaysVolume = 0;
                    // Do we need to switch contracts?
                    switch (cf.RolloverType) {
                        case ContinuousFuturesRolloverType.Time:
                            if (MyUtils.BusinessDaysBetween(currentDate, frontFuture.Expiration.Value, calendar) <= cf.RolloverDays) {
                                switchContract = true;
                            }
                            break;

                        case ContinuousFuturesRolloverType.Volume:
                            if (backData != null && backDailyVolume.Last() > frontDailyVolume.Last())
                                counter++;
                            else
                                counter = 0;
                            switchContract = counter >= cf.RolloverDays;
                            break;

                        case ContinuousFuturesRolloverType.OpenInterest:
                            if (backData != null && backDailyOpenInterest.Last() > frontDailyOpenInterest.Last())
                                counter++;
                            else
                                counter = 0;
                            switchContract = counter >= cf.RolloverDays;
                            break;

                        case ContinuousFuturesRolloverType.VolumeAndOpenInterest:
                            if (backData != null && backDailyOpenInterest.Last() > frontDailyOpenInterest.Last() &&
                                backDailyVolume.Last() > frontDailyVolume.Last())
                                counter++;
                            else
                                counter = 0;
                            switchContract = counter >= cf.RolloverDays;
                            break;

                        case ContinuousFuturesRolloverType.VolumeOrOpenInterest:
                            if (backData != null && backDailyOpenInterest.Last() > frontDailyOpenInterest.Last() ||
                                backDailyVolume.Last() > frontDailyVolume.Last())
                                counter++;
                            else
                                counter = 0;
                            switchContract = counter >= cf.RolloverDays;
                            break;
                    }
                }

                if (frontFuture.Expiration.Value <= currentDate) {
                    // No matter what, obviously we need to switch if the contract expires
                    switchContract = true;
                }

                // Finally if we have simply run out of data, we're forced to switch
                if (frontData.ReachedEndOfSeries) {
                    switchContract = true;
                }
                // Finally advance the time and indices...keep moving forward until the selected series has moved
                frontData.NextBar();
                currentDate = frontData[0].DT;

                backData?.AdvanceTo(currentDate);
                selectedData.AdvanceTo(currentDate);
                // This next check here is necessary for the time-based switchover to work after weekends or holidays
                if (cf.RolloverType == ContinuousFuturesRolloverType.Time &&
                    MyUtils.BusinessDaysBetween(currentDate, frontFuture.Expiration.Value, calendar) <= cf.RolloverDays) {
                    switchContract = true;
                }
                // We switch to the next contract
                if (switchContract) {
                    // Make any required price adjustments
                    decimal adjustmentFactor;
                    if (cf.AdjustmentMode == ContinuousFuturesAdjustmentMode.Difference) {
                        adjustmentFactor = backData[0].Close - frontData[0].Close;
                        foreach (var bar in cfData) {
                            AdjustBar(bar, adjustmentFactor, cf.AdjustmentMode);
                        }
                    }
                    else if (cf.AdjustmentMode == ContinuousFuturesAdjustmentMode.Ratio) {
                        adjustmentFactor = backData[0].Close / frontData[0].Close;
                        foreach (var bar in cfData) {
                            AdjustBar(bar, adjustmentFactor, cf.AdjustmentMode);
                        }
                    }
                    // Update the contracts
                    var prevFront = frontFuture;
                    frontFuture = backFuture;
                    backFuture = futures.FirstOrDefault(x => x.Expiration > backFuture.Expiration);
                    var prevSelected = selectedFuture;
                    selectedFuture = futures.Where(x => x.Expiration >= frontFuture.Expiration).ElementAtOrDefault(cf.Month - 1);

                    _logger.Info(
                        $"CFB Filling request for {request.Instrument.Symbol}: switching front contract from {prevFront.Symbol} to {frontFuture.Symbol} (selected contract from {prevSelected.Symbol} to {(selectedFuture == null ? string.Empty : selectedFuture.Symbol)}) at {currentDate.ToString("yyyy-MM-dd")}");


                    if (frontFuture == null) break; //no other futures left, get out
                    if (selectedFuture == null) break;

                    lock (_dataLock) {
                        frontData = new TimeSeries(_data[new KeyValuePair<int, BarSize>(frontFuture.ID.Value, request.Frequency)]);
                        backData = backFuture != null ? new TimeSeries(_data[new KeyValuePair<int, BarSize>(backFuture.ID.Value, request.Frequency)]) : null;
                        selectedData = new TimeSeries(_data[new KeyValuePair<int, BarSize>(selectedFuture.ID.Value, request.Frequency)]);
                    }

                    frontData.AdvanceTo(currentDate);

                    backData?.AdvanceTo(currentDate);

                    selectedData.AdvanceTo(currentDate);

                    //TODO make sure that the data series actually cover the current date
                    switchContract = false;
                    lastUsedSelectedFuture = selectedFuture;
                }

                cfData.Add(selectedData[0]);
            }
            //clean up
            _contracts.Remove(request.AssignedID);
            //throw out any data from before the start of the request
            cfData = cfData.Where(x => x.DT >= request.StartingDate && x.DT <= request.EndingDate).ToList();
            //we're done, so just raise the event
            if (raiseDataEvent)
                RaiseEvent(HistoricalDataArrived, this, new HistoricalDataEventArgs(request, cfData));
            //clean up some data!
            lock (_dataUsesLock) {
                foreach (var i in futures) {
                    var kvp = new KeyValuePair<int, BarSize>(i.ID.Value, request.Frequency);
                    if (_dataUsesPending[kvp] == 1) //this data isn't needed anywhere else, we can delete it
                    {
                        _dataUsesPending.Remove(kvp);
                        lock (_dataLock) {
                            _data.Remove(kvp);
                        }
                    }
                    else {
                        _dataUsesPending[kvp]--;
                    }
                }
            }

            return lastUsedSelectedFuture;
        }

        private void AdjustBar(OHLCBar bar, decimal adjustmentFactor, ContinuousFuturesAdjustmentMode mode)
        {
            switch (mode) {
                case ContinuousFuturesAdjustmentMode.NoAdjustment:
                    return;
                case ContinuousFuturesAdjustmentMode.Difference:
                    bar.Open += adjustmentFactor;
                    bar.High += adjustmentFactor;
                    bar.Low += adjustmentFactor;
                    bar.Close += adjustmentFactor;
                    break;
                case ContinuousFuturesAdjustmentMode.Ratio:
                    bar.Open *= adjustmentFactor;
                    bar.High *= adjustmentFactor;
                    bar.Low *= adjustmentFactor;
                    bar.Close *= adjustmentFactor;
                    break;
            }
        }

        private static void RaiseEvent<T>(EventHandler<T> @event, object sender, T e)
            where T : EventArgs
        {
            @event?.Invoke(sender, e);
        }

        /// <summary>
        ///     Process a FrontContractRequest
        ///     CFs with a time-based switchover are calculated on the spot
        ///     CFs with other types of switchover require data, so we send off the appropriate data requests here
        /// </summary>
        private void ProcessFrontContractRequest(FrontContractRequest request)
        {
            _logger.Info($"Processing front contract request for symbol: {request.Instrument.Symbol} at: {(request.Date.HasValue ? request.Date.ToString() : "Now")}");

            if (request.Instrument.ContinuousFuture.RolloverType == ContinuousFuturesRolloverType.Time) {
                ProcessTimeBasedFrontContractRequest(request);
            }
            else //otherwise, we have to actually look at the historical data to figure out which contract is selected
            {
                ProcessDataBasedFrontContractRequest(request);
            }
        }

        /// <summary>
        ///     Finds the front contract for continuous futures with non-time-based roll.
        /// </summary>
        private void ProcessDataBasedFrontContractRequest(FrontContractRequest request)
        {
            var currentDate = request.Date ?? DateTime.Now;
            //this is a tough one, because it needs to be asynchronous (historical
            //data can take a long time to download).
            var r = new Random();
            //we use GetRequiredRequests to get the historical requests we need to make
            var tmpReq = new HistoricalDataRequest
            {
                Instrument = request.Instrument,
                StartingDate = currentDate.AddDays(-1),
                EndingDate = currentDate,
                Frequency = BarSize.OneDay
            };
            //give the request a unique id
            lock (_requestsLock) {
                int id;
                do {
                    id = r.Next();
                } while (_requests.ContainsKey(id));
                tmpReq.AssignedID = id;
                _requests.Add(tmpReq.AssignedID, tmpReq);
            }

            var reqs = GetRequiredRequests(tmpReq);
            //make sure the request is fulfillable with the available contracts, otherwise return empty-handed
            if (reqs.Count == 0 || reqs.Count(x => x.Instrument.Expiration.HasValue && x.Instrument.Expiration.Value >= request.Date) == 0) {
                lock (_frontContractReturnLock) {
                    RaiseEvent(FoundFrontContract, this, new FoundFrontContractEventArgs(request.ID, null, currentDate));
                }
                lock (_requestsLock) {
                    _requests.Remove(tmpReq.AssignedID);
                }
                return;
            }
            // add it to the collection of requests so we can access it later
            _requestTypes.Add(tmpReq.AssignedID, false);
            //add it to the front contract requests map
            _frontContractRequestMap.Add(tmpReq.AssignedID, request);
            //finally send out a request for all the data...when it arrives,
            //we process it and return the required front future
            foreach (var req in reqs) {
                lock (_dataUsesLock) {
                    var kvp = new KeyValuePair<int, BarSize>(req.Instrument.ID.Value, req.Frequency);
                    if (_dataUsesPending.ContainsKey(kvp)) {
                        _dataUsesPending[kvp]++;
                    }
                    else {
                        _dataUsesPending.Add(kvp, 1);
                    }
                }

                var requestID = _client.RequestHistoricalData(req);

                _histReqIDMap.Add(requestID, tmpReq.AssignedID);
            }
        }

        /// <summary>
        ///     Finds the front contract for continuous futures with time-based roll.
        /// </summary>
        private void ProcessTimeBasedFrontContractRequest(FrontContractRequest request)
        {
            var currentDate = request.Date ?? DateTime.Now;
            var cf = request.Instrument.ContinuousFuture;
            //if the roll-over is time based, we can find the appropriate contract programmatically
            var selectedDate = currentDate;

            while (!cf.MonthIsUsed(selectedDate.Month)) {
                selectedDate = selectedDate.AddMonths(1);
            }

            var currentMonthsExpirationDate = cf.UnderlyingSymbol.ExpirationDate(selectedDate.Year, selectedDate.Month);
            var switchOverDate = currentMonthsExpirationDate;
            var calendar = MyUtils.GetCalendarFromCountryCode("US");
            //the front contract
            //find the switchover date
            var daysBack = cf.RolloverDays;
            while (daysBack > 0) {
                switchOverDate = switchOverDate.AddDays(-1);
                if (calendar.isBusinessDay(switchOverDate)) {
                    daysBack--;
                }
            }
            //this month's contract has already been switched to the next one
            var monthsLeft = 1;
            var count = 0;
            if (currentDate >= switchOverDate) {
                while (monthsLeft > 0) {
                    count++;
                    if (cf.MonthIsUsed(selectedDate.AddMonths(count).Month)) {
                        monthsLeft--;
                    }
                }
                selectedDate = selectedDate.AddMonths(count);
            }
            //we found the "front" month, no go back the required number of months
            //while skipping unused months
            monthsLeft = cf.Month - 1;
            count = 0;

            while (monthsLeft > 0) {
                if (cf.MonthIsUsed(selectedDate.AddMonths(count).Month)) {
                    monthsLeft--;
                }
                count++;
            }

            selectedDate = selectedDate.AddMonths(count);
            //we got the month we want! find the contract
            var searchFunc = new Func<Instrument, bool>(
                x =>
                    x.Expiration.HasValue &&
                    x.Expiration.Value.Month == selectedDate.Month &&
                    x.Expiration.Value.Year == selectedDate.Year &&
                    x.UnderlyingSymbol == cf.UnderlyingSymbol.Symbol);

            var contract = _instrumentMgr.FindInstruments(pred: searchFunc).FirstOrDefault();
            var timer = new Timer(50) {AutoReset = false};

            timer.Elapsed += (sender, e) =>
                {
                    lock (_frontContractReturnLock) {
                        RaiseEvent(FoundFrontContract, this, new FoundFrontContractEventArgs(request.ID, contract, currentDate));
                    }
                };
            timer.Start();
        }

        public event PropertyChangedEventHandler PropertyChanged;

        [NotifyPropertyChangedInvocator]
        protected virtual void OnPropertyChanged([CallerMemberName] string propertyName = null)
        {
            PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(propertyName));
        }
    }
}

#pragma warning restore 67