// -----------------------------------------------------------------------
// <copyright file="QDMSClient.cs" company="">
// Copyright 2014 Alexander Soffronow Pagonidis
// </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;

using LZ4;

using NetMQ;
using NetMQ.Sockets;

using ProtoBuf;

using QDMS;

namespace QDMSClient
{
    // ReSharper disable once InconsistentNaming
    public class QDMSClient : IDataClient
    {
        #region Variables
        // Where to connect
        private readonly string _host;
        private readonly int _realTimeRequestPort;
        private readonly int _realTimePublishPort;
        private readonly int _instrumentServerPort;
        private readonly int _historicalDataPort;
        /// <summary>
        /// This holds the zeromq identity string that we'll be using.
        /// </summary>
        private readonly string _name;
        /// <summary>
        /// Queue of historical data requests waiting to be sent out.
        /// </summary>
        private readonly ConcurrentQueue<HistoricalDataRequest> _historicalDataRequests;
        private readonly object _reqSocketLock = new object();
        private readonly object _subSocketLock = new object();
        private readonly object _dealerSocketLock = new object();
        private readonly object _pendingHistoricalRequestsLock = new object();
        private readonly object _realTimeDataStreamsLock = new object();

        /// <summary>
        /// This socket sends requests for real time data.
        /// </summary>
        private DealerSocket _reqSocket;
        /// <summary>
        /// This socket receives real time data.
        /// </summary>
        private SubscriberSocket _subSocket;
        /// <summary>
        /// This socket sends requests for and receives historical data.
        /// </summary>
        private DealerSocket _dealerSocket;
        /// <summary>
        /// Pooler class to manage all used sockets.
        /// </summary>
        private NetMQPoller _poller;
        /// <summary>
        /// Periodically sends heartbeat messages to server to ensure the connection is up.
        /// </summary>
        private NetMQTimer _heartBeatTimer;
        /// <summary>
        /// The time when the last heartbeat was received. If it's too long ago we're disconnected.
        /// </summary>
        private DateTime _lastHeartBeat;
        /// <summary>
        /// This thread run the DealerLoop() method.
        /// It sends out requests for historical data, and receives data when requests are fulfilled.
        /// </summary>
        private Thread _dealerLoopThread;
        /// <summary>
        /// This int is used to give each historical request a unique RequestID.
        /// Keep in mind this is unique to the CLIENT. AssignedID is unique to the server.
        /// </summary>
        private int _requestCount;
        /// <summary>
        /// Used to start and stop the various threads that keep the client running.
        /// </summary>
        private bool _running;
        private bool _disposed;
        #endregion

        /// <summary>
        /// Returns true if the connection to the server is up.
        /// </summary>
        public bool Connected => (DateTime.Now - _lastHeartBeat).TotalSeconds < 5;

        public bool ClientConnected => (_poller != null) && _poller.IsRunning && ((DateTime.Now - _lastHeartBeat).TotalSeconds < 5);

        /// <summary>
        /// Keeps track of historical requests that have been sent but the data has not been received yet.
        /// </summary>
        public ObservableCollection<HistoricalDataRequest> PendingHistoricalRequests { get; }

        /// <summary>
        /// Keeps track of live real time data streams.
        /// </summary>
        public ObservableCollection<RealTimeDataRequest> RealTimeDataStreams { get; }

        #region IDisposable implementation
        public void Dispose()
        {
            if (_disposed) {
                return;
            }

            Disconnect();

            _reqSocket?.Dispose();
            _subSocket?.Dispose();
            _dealerSocket?.Dispose();
            _heartBeatTimer?.Dispose();
            _poller?.Dispose();

            _disposed = true;
        }
        #endregion

        #region IDataClient implementation
        public event EventHandler<RealTimeDataEventArgs> RealTimeDataReceived;

        public event EventHandler<HistoricalDataEventArgs> HistoricalDataReceived;

        public event EventHandler<LocallyAvailableDataInfoReceivedEventArgs> LocallyAvailableDataInfoReceived;

        public event EventHandler<ErrorArgs> Error;

        /// <summary>
        /// Pushes data to local storage.
        /// </summary>
        public void PushData(DataAdditionRequest request)
        {
            CheckDisposed();

            if (request.Instrument?.ID == null) {
                RaiseEvent(Error, null, new ErrorArgs(-1, "Instrument must be set and have an ID."));

                return;
            }

            using (var ms = new MemoryStream()) {
                lock (_dealerSocketLock) {
                    _dealerSocket.SendMoreFrame("HISTPUSH");
                    _dealerSocket.SendFrame(MyUtils.ProtoBufSerialize(request, ms));
                }
            }
        }

        /// <summary>
        /// Requests information on what historical data is available in local storage for this instrument.
        /// </summary>
        public void GetLocallyAvailableDataInfo(Instrument instrument)
        {
            CheckDisposed();

            lock (_dealerSocketLock) {
                _dealerSocket.SendMoreFrame("AVAILABLEDATAREQ");

                using (var ms = new MemoryStream()) {
                    _dealerSocket.SendFrame(MyUtils.ProtoBufSerialize(instrument, ms));
                }
            }
        }

        /// <summary>
        /// Request historical data. Data will be delivered through the HistoricalDataReceived event.
        /// </summary>
        /// <returns>An ID uniquely identifying this historical data request. -1 if there was an error.</returns>
        public int RequestHistoricalData(HistoricalDataRequest request)
        {
            CheckDisposed();
            // Make sure the request is valid
            if (request.EndingDate < request.StartingDate) {
                RaiseEvent(Error, this, new ErrorArgs(-1, "Historical Data Request Failed: Starting date must be after ending date."));

                return -1;
            }

            if (request.Instrument == null) {
                RaiseEvent(Error, this, new ErrorArgs(-1, "Historical Data Request Failed: null Instrument."));

                return -1;
            }

            if (!Connected) {
                RaiseEvent(Error, this, new ErrorArgs(-1, "Could not request historical data - not connected."));

                return -1;
            }

            if (!request.RTHOnly && request.Frequency >= BarSize.OneDay && request.DataLocation != DataLocation.ExternalOnly) {
                RaiseEvent(
                    Error,
                    this,
                    new ErrorArgs(
                        -1,
                        "Warning: Requesting low-frequency data outside RTH should be done with DataLocation = ExternalOnly, data from local storage will be incorrect."));
            }

            request.RequestID = _requestCount++;

            lock (_pendingHistoricalRequestsLock) {
                PendingHistoricalRequests.Add(request);
            }

            _historicalDataRequests.Enqueue(request);

            return request.RequestID;
        }

        /// <summary>
        /// Request a new real time data stream. Data will be delivered through the RealTimeDataReceived event.
        /// </summary>
        /// <returns>An ID uniquely identifying this real time data request. -1 if there was an error.</returns>
        public int RequestRealTimeData(RealTimeDataRequest request)
        {
            CheckDisposed();

            if (!Connected) {
                RaiseEvent(Error, this, new ErrorArgs(-1, "Could not request real time data - not connected."));
                return -1;
            }

            if (request.Instrument == null) {
                RaiseEvent(Error, this, new ErrorArgs(-1, "Real Time Data Request Failed: null Instrument."));
                return -1;
            }

            request.RequestID = _requestCount++;

            using (var ms = new MemoryStream()) {
                lock (_reqSocketLock) {
                    // Two part message:
                    // 1: "RTD"
                    // 2: serialized RealTimeDataRequest
                    _reqSocket.SendMoreFrame(string.Empty);
                    _reqSocket.SendMoreFrame("RTD");
                    _reqSocket.SendFrame(MyUtils.ProtoBufSerialize(request, ms));
                }
            }

            return request.RequestID;
        }

        /// <summary>
        /// Tries to connect to the QDMS server.
        /// </summary>
        public void Connect()
        {
            CheckDisposed();

            if (ClientConnected) {
                return;
            }

            lock (_reqSocketLock) {
                _reqSocket = new DealerSocket($"tcp://{_host}:{_realTimeRequestPort}");
                _reqSocket.Options.Identity = Encoding.UTF8.GetBytes(_name);
                // Start off by sending a ping to make sure everything is regular
                var reply = string.Empty;

                try {
                    _reqSocket.SendMoreFrame(string.Empty).SendFrame("PING");

                    reply = _reqSocket.ReceiveFrameString(); // Empty frame starts the REP message //todo receive string?

                    if (reply != null) {
                        reply = _reqSocket.ReceiveFrameString();
                    }
                }
                catch {
                    Disconnect();
                }

                if (reply != null && !reply.Equals("PONG", StringComparison.InvariantCultureIgnoreCase)) {
                    try {
                        _reqSocket.Disconnect($"tcp://{_host}:{_realTimeRequestPort}");
                    }
                    finally {
                        _reqSocket.Close();
                        _reqSocket = null;
                    }

                    RaiseEvent(Error, this, new ErrorArgs(-1, "Could not connect to server."));

                    return;
                }

                _reqSocket.ReceiveReady += ReqSocketReceiveReady;
            }

            lock (_subSocketLock) {
                _subSocket = new SubscriberSocket($"tcp://{_host}:{_realTimePublishPort}");
                _subSocket.Options.Identity = Encoding.UTF8.GetBytes(_name);
                _subSocket.ReceiveReady += SubSocketReceiveReady;
            }

            lock (_dealerSocketLock) {
                _dealerSocket = new DealerSocket($"tcp://{_host}:{_historicalDataPort}");
                _dealerSocket.Options.Identity = Encoding.UTF8.GetBytes(_name);
                _dealerSocket.ReceiveReady += DealerSocketReceiveReady;
            }

            _lastHeartBeat = DateTime.Now;
            _running = true;
            // This loop sends out historical data requests and receives the data
            _dealerLoopThread = new Thread(DealerLoop) {Name = "Client Dealer Loop"};
            _dealerLoopThread.Start();

            _heartBeatTimer = new NetMQTimer(TimeSpan.FromSeconds(1));
            _heartBeatTimer.Elapsed += TimerElapsed;

            // This loop takes care of replies to the request socket: heartbeats and data request status messages
            _poller = new NetMQPoller {_reqSocket, _subSocket, _dealerSocket, _heartBeatTimer};

            _poller.RunAsync();
        }

        /// <summary>
        /// Disconnects from the server.
        /// </summary>
        public void Disconnect(bool cancelStreams = true)
        {
            CheckDisposed();
            // Start by canceling all active real time streams
            if (cancelStreams) {
                while (RealTimeDataStreams.Count > 0) {
                    CancelRealTimeData(RealTimeDataStreams.First().Instrument);
                }
            }

            _running = false;

            if (_poller != null && _poller.IsRunning) {
                _poller.Stop();
            }

            _heartBeatTimer?.Stop();

            if (_dealerLoopThread != null && _dealerLoopThread.ThreadState == ThreadState.Running)
                _dealerLoopThread.Join(10);

            if (_reqSocket != null) {
                try {
                    _reqSocket.Disconnect($"tcp://{_host}:{_realTimeRequestPort}");
                }
                catch {
                    _reqSocket.Dispose();
                    _reqSocket = null;
                }
            }

            if (_subSocket != null) {
                try {
                    _subSocket.Disconnect($"tcp://{_host}:{_realTimePublishPort}");
                }
                catch {
                    _subSocket.Dispose();
                    _subSocket = null;
                }
            }

            if (_dealerSocket != null) {
                try {
                    _dealerSocket.Disconnect($"tcp://{_host}:{_historicalDataPort}");
                }
                catch {
                    _dealerSocket.Dispose();
                    _dealerSocket = null;
                }
            }
        }

        /// <summary>
        /// Query the server for contracts matching a particular set of features.
        /// </summary>
        /// <param name="instrument">An Instrument object; any features that are not null will be search parameters. If null, all instruments are returned.</param>
        /// <returns>A list of instruments matching these features.</returns>
        public List<Instrument> FindInstruments(Instrument instrument = null)
        {
            CheckDisposed();

            if (!Connected) {
                RaiseEvent(Error, this, new ErrorArgs(-1, "Could not request instruments - not connected."));

                return new List<Instrument>();
            }

            using (var s = new RequestSocket($"tcp://{_host}:{_instrumentServerPort}")) {
                using (var ms = new MemoryStream()) {
                    if (instrument == null) // All contracts
                    {
                        s.SendFrame("ALL");
                    }
                    else // An actual search
                    {
                        s.SendMoreFrame("SEARCH"); // First we send a search request

                        // Then we need to serialize and send the instrument
                        s.SendFrame(MyUtils.ProtoBufSerialize(instrument, ms));
                    }

                    // First we receive the size of the final uncompressed byte[] array
                    bool hasMore;
                    var sizeBuffer = s.ReceiveFrameBytes(out hasMore);
                    if (sizeBuffer.Length == 0) {
                        RaiseEvent(Error, this, new ErrorArgs(-1, "Contract request failed, received no reply."));
                        return new List<Instrument>();
                    }

                    var outputSize = BitConverter.ToInt32(sizeBuffer, 0);

                    // Then the actual data
                    var buffer = s.ReceiveFrameBytes(out hasMore);
                    if (buffer.Length == 0) {
                        RaiseEvent(Error, this, new ErrorArgs(-1, "Contract request failed, received no data."));
                        return new List<Instrument>();
                    }

                    try {
                        // Then we process it by first decompressing
                        ms.SetLength(0);
                        var decoded = LZ4Codec.Decode(buffer, 0, buffer.Length, outputSize);
                        ms.Write(decoded, 0, decoded.Length);
                        ms.Position = 0;

                        // And finally deserializing
                        return Serializer.Deserialize<List<Instrument>>(ms);
                    }
                    catch (Exception ex) {
                        RaiseEvent(Error, this, new ErrorArgs(-1, "Error processing instrument data: " + ex.Message));
                        return new List<Instrument>();
                    }
                }
            }
        }

        /// <summary>
        /// Cancel a live real time data stream.
        /// </summary>
        public void CancelRealTimeData(Instrument instrument)
        {
            CheckDisposed();

            if (!Connected) {
                RaiseEvent(Error, this, new ErrorArgs(-1, "Could not cancel real time data - not connected."));

                return;
            }

            if (_reqSocket != null) {
                lock (_reqSocketLock) {
                    // Two part message:
                    // 1: "CANCEL"
                    // 2: serialized Instrument object
                    using (var ms = new MemoryStream()) {
                        _reqSocket.SendMoreFrame("");
                        _reqSocket.SendMoreFrame("CANCEL");
                        _reqSocket.SendFrame(MyUtils.ProtoBufSerialize(instrument, ms));
                    }
                }
            }

            _subSocket?.Unsubscribe(Encoding.UTF8.GetBytes(instrument.Symbol));

            lock (_realTimeDataStreamsLock) {
                RealTimeDataStreams.RemoveAll(x => x.Instrument.ID == instrument.ID);
            }
        }

        /// <summary>
        /// Get a list of all available instruments
        /// </summary>
        /// <returns></returns>
        public List<Instrument> GetAllInstruments()
        {
            CheckDisposed();

            return FindInstruments();
        }
        #endregion

        /// <summary>
        /// Initialization constructor.
        /// </summary>
        /// <param name="clientName">The name of this client. Should be unique. Used to route historical data.</param>
        /// <param name="host">The address of the server.</param>
        /// <param name="realTimeRequestPort">The port used for real time data requsts.</param>
        /// <param name="realTimePublishPort">The port used for publishing new real time data.</param>
        /// <param name="instrumentServerPort">The port used by the instruments server.</param>
        /// <param name="historicalDataPort">The port used for historical data.</param>
        public QDMSClient(string clientName, string host, int realTimeRequestPort, int realTimePublishPort, int instrumentServerPort, int historicalDataPort)
        {
            _host = host;
            _name = clientName;
            _realTimeRequestPort = realTimeRequestPort;
            _realTimePublishPort = realTimePublishPort;
            _instrumentServerPort = instrumentServerPort;
            _historicalDataPort = historicalDataPort;


            _historicalDataRequests = new ConcurrentQueue<HistoricalDataRequest>();
            PendingHistoricalRequests = new ObservableCollection<HistoricalDataRequest>();
            RealTimeDataStreams = new ObservableCollection<RealTimeDataRequest>();
        }

        /// <summary>
        /// Add an instrument to QDMS.
        /// </summary>
        /// <param name="instrument"></param>
        /// <returns>The instrument with its ID set if successful, null otherwise.</returns>
        public Instrument AddInstrument(Instrument instrument)
        {
            CheckDisposed();

            if (!Connected) {
                RaiseEvent(Error, this, new ErrorArgs(-1, "Could not add instrument - not connected."));
                return null;
            }

            if (instrument == null) {
                RaiseEvent(Error, this, new ErrorArgs(-1, "Could not add instrument - instrument is null."));
                return null;
            }

            using (var s = new RequestSocket($"tcp://{_host}:{_instrumentServerPort}")) {
                using (var ms = new MemoryStream()) {

                    s.SendMoreFrame("ADD"); // First we send an "ADD" request

                    // Then we need to serialize and send the instrument
                    s.SendFrame(MyUtils.ProtoBufSerialize(instrument, ms));

                    // Then get the reply
                    var result = s.ReceiveString(TimeSpan.FromSeconds(1));

                    if (result != "SUCCESS") {
                        RaiseEvent(Error, this, new ErrorArgs(-1, "Instrument addition failed: received no reply."));

                        return null;
                    }

                    // Addition was successful, receive the instrument and return it
                    var serializedInstrument = s.ReceiveFrameBytes();

                    return MyUtils.ProtoBufDeserialize<Instrument>(serializedInstrument, ms);
                }
            }
        }

        // The timer sends heartbeat messages so we know that the server is still up.
        private void TimerElapsed(object sender, NetMQTimerEventArgs e)
        {
            lock (_reqSocketLock) {
                _reqSocket.SendMoreFrame(string.Empty);
                _reqSocket.SendFrame("PING");
            }
        }

        /// <summary>
        /// Process replies on the request socket.
        /// Heartbeats, errors, and subscribing to real time data streams.
        /// </summary>
        private void ReqSocketReceiveReady(object sender, NetMQSocketEventArgs e)
        {
            using (var ms = new MemoryStream()) {
                lock (_reqSocketLock) {
                    var reply = _reqSocket.ReceiveFrameString();

                    if (reply == null) return;

                    reply = _reqSocket.ReceiveFrameString();

                    switch (reply) {
                        case "PONG": // Reply to heartbeat message
                            _lastHeartBeat = DateTime.Now;
                            break;

                        case "ERROR": // Something went wrong
                            {
                            // First the message
                            var error = _reqSocket.ReceiveFrameString();

                            // Then the request
                            var buffer = _reqSocket.ReceiveFrameBytes();
                            var request = MyUtils.ProtoBufDeserialize<RealTimeDataRequest>(buffer, ms);

                            // Error event
                            RaiseEvent(Error, this, new ErrorArgs(-1, "Real time data request error: " + error, request.RequestID));

                            return;
                            }
                        case "SUCCESS": // Successful request to start a new real time data stream
                            {
                            // Receive the request
                            var buffer = _reqSocket.ReceiveFrameBytes();
                            var request = MyUtils.ProtoBufDeserialize<RealTimeDataRequest>(buffer, ms);

                            // Add it to the active streams
                            lock (_realTimeDataStreamsLock) {
                                RealTimeDataStreams.Add(request);
                            }

                            // Request worked, so we subscribe to the stream
                            _subSocket.Subscribe(BitConverter.GetBytes(request.Instrument.ID.Value));
                            }
                            break;

                        case "CANCELED": // Successful cancelation of a real time data stream
                            {
                            // Also receive the symbol
                            var symbol = _reqSocket.ReceiveFrameString();
                            // Nothing to do?
                            }
                            break;
                    }
                }
            }
        }

        /// <summary>
        /// Dealer socket sends out requests for historical data and raises an event when it's received
        /// </summary>
        private void DealerLoop()
        {
            using (var ms = new MemoryStream()) {
                try {
                    while (_running) {
                        // Send any pending historical data requests
                        lock (_dealerSocketLock) {
                            SendQueuedHistoricalRequests(ms);
                        }

                        Thread.Sleep(10);
                    }
                }
                catch {
                    Dispose();
                }
            }
        }

        private void SendQueuedHistoricalRequests(MemoryStream ms)
        {
            while (!_historicalDataRequests.IsEmpty) {
                HistoricalDataRequest request;

                if (_historicalDataRequests.TryDequeue(out request)) {
                    var buffer = MyUtils.ProtoBufSerialize(request, ms);

                    _dealerSocket.SendMoreFrame("HISTREQ");
                    _dealerSocket.SendFrame(buffer);
                }
            }
        }

        private void SubSocketReceiveReady(object sender, NetMQSocketEventArgs e)
        {
            bool hasMore;

            _subSocket.ReceiveFrameBytes(out hasMore);

            if (hasMore) {
                var buffer = _subSocket.ReceiveFrameBytes();
                var bar = MyUtils.ProtoBufDeserialize<RealTimeDataEventArgs>(buffer, new MemoryStream());

                RaiseEvent(RealTimeDataReceived, null, bar);
            }
        }

        /// <summary>
        /// Handling replies to a data push, a historical data request, or an available data request
        /// </summary>
        private void DealerSocketReceiveReady(object sender, NetMQSocketEventArgs e)
        {
            lock (_dealerSocketLock) {
                // 1st message part: what kind of stuff we're receiving
                var type = _dealerSocket.ReceiveFrameString();

                switch (type) {
                    case "PUSHREP":
                        HandleDataPushReply();
                        break;

                    case "HISTREQREP":
                        HandleHistoricalDataRequestReply();
                        break;

                    case "AVAILABLEDATAREP":
                        HandleAvailabledataReply();
                        break;

                    case "ERROR":
                        HandleErrorReply();
                        break;
                }
            }
        }

        /// <summary>
        /// Called when we get some sort of error reply
        /// </summary>
        private void HandleErrorReply()
        {
            // The request ID
            bool hasMore;
            var buffer = _dealerSocket.ReceiveFrameBytes(out hasMore);
            if (!hasMore) return;
            var requestId = BitConverter.ToInt32(buffer, 0);

            // Remove from pending requests
            lock (_pendingHistoricalRequestsLock) {
                PendingHistoricalRequests.RemoveAll(x => x.RequestID == requestId);
            }

            // Finally the error message
            var message = _dealerSocket.ReceiveFrameString();
            // Raise the error event
            RaiseEvent(Error, this, new ErrorArgs(-1, message, requestId));
        }

        /// <summary>
        /// Called when we get a reply on a request for available data in local storage.
        /// </summary>
        private void HandleAvailabledataReply()
        {
            // First the instrument
            using (var ms = new MemoryStream()) {
                var buffer = _dealerSocket.ReceiveFrameBytes();
                var instrument = MyUtils.ProtoBufDeserialize<Instrument>(buffer, ms);
                // Second the number of items
                buffer = _dealerSocket.ReceiveFrameBytes();
                var count = BitConverter.ToInt32(buffer, 0);
                // Then actually get the items, if any
                if (count == 0) {
                    RaiseEvent(LocallyAvailableDataInfoReceived, this, new LocallyAvailableDataInfoReceivedEventArgs(instrument, new List<StoredDataInfo>()));
                }
                else {
                    var storageInfo = new List<StoredDataInfo>();
                    for (var i = 0;i < count;i++) {
                        buffer = _dealerSocket.ReceiveFrameBytes();
                        var info = MyUtils.ProtoBufDeserialize<StoredDataInfo>(buffer, ms);
                        storageInfo.Add(info);

                        if (!_dealerSocket.Options.ReceiveMore) break;
                    }

                    RaiseEvent(LocallyAvailableDataInfoReceived, this, new LocallyAvailableDataInfoReceivedEventArgs(instrument, storageInfo));
                }
            }
        }

        /// <summary>
        /// Called on a reply to a data push
        /// </summary>
        private void HandleDataPushReply()
        {
            var result = _dealerSocket.ReceiveFrameString();

            if (result == "OK") // Everything is alright
            {}
            else if (result == "ERROR") {
                // Receive the error
                var error = _dealerSocket.ReceiveFrameString();
                RaiseEvent(Error, this, new ErrorArgs(-1, "Data push error: " + error));
            }
        }

        /// <summary>
        /// Called ona reply to a historical data request
        /// </summary>
        private void HandleHistoricalDataRequestReply()
        {
            var ms = new MemoryStream();
            // 2nd message part: the HistoricalDataRequest object that was used to make the request
            bool hasMore;
            var requestBuffer = _dealerSocket.ReceiveFrameBytes(out hasMore);
            if (!hasMore) return;

            var request = MyUtils.ProtoBufDeserialize<HistoricalDataRequest>(requestBuffer, ms);
            // 3rd message part: the size of the uncompressed, serialized data. Necessary for decompression.
            var sizeBuffer = _dealerSocket.ReceiveFrameBytes(out hasMore);
            if (!hasMore) return;

            var outputSize = BitConverter.ToInt32(sizeBuffer, 0);
            // 4th message part: the compressed serialized data.
            var dataBuffer = _dealerSocket.ReceiveFrameBytes();
            var decompressed = LZ4Codec.Decode(dataBuffer, 0, dataBuffer.Length, outputSize);
            var data = MyUtils.ProtoBufDeserialize<List<OHLCBar>>(decompressed, ms);
            // Remove from pending requests
            lock (_pendingHistoricalRequestsLock) {
                PendingHistoricalRequests.RemoveAll(x => x.RequestID == request.RequestID);
            }

            RaiseEvent(HistoricalDataReceived, this, new HistoricalDataEventArgs(request, data));
        }

        /// <summary>
        /// Raise the event in a threadsafe manner
        /// </summary>
        /// <param name="event"></param>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        /// <typeparam name="T"></typeparam>
        private static void RaiseEvent<T>(EventHandler<T> @event, object sender, T e)
            where T : EventArgs
        {
            var handler = @event;

            handler?.Invoke(sender, e);
        }

        private void CheckDisposed()
        {
            if (_disposed) {
                throw new ObjectDisposedException("QDMSClient");
            }
        }
    }
}