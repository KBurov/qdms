// -----------------------------------------------------------------------
// <copyright file="HistoricalDataServer.cs" company="">
// Copyright 2014 Alexander Soffronow Pagonidis
// </copyright>
// -----------------------------------------------------------------------

// This class handles networking for historical data.
// Clients send their requests through ZeroMQ. Here they are parsed
// and then forwarded to the HistoricalDataBroker.
// Data sent from the HistoricalDataBroker is sent out to the clients.
// Three types of possible requests:
// 1. For historical data
// 2. To check what data is available in the local database
// 3. To add data to the local database

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;

using LZ4;

using NetMQ;
using NetMQ.Sockets;

using NLog;

using ProtoBuf;

using QDMS;
// ReSharper disable once CheckNamespace
namespace QDMSServer
{
    public class HistoricalDataServer : IDisposable
    {
        private readonly Logger _logger = LogManager.GetCurrentClassLogger();
        private readonly int _listenPort;
        private readonly IHistoricalDataBroker _broker;
        private readonly ManualResetEventSlim _socketFree = new ManualResetEventSlim(true);

        private NetMQSocket _routerSocket;
        private NetMQPoller _poller;
        private bool _disposed;

        /// <summary>
        ///     Whether the broker is running or not.
        /// </summary>
        public bool ServerRunning => _poller != null && _poller.IsRunning;

        #region IDisposable implementation
        public void Dispose()
        {
            if (_disposed) {
                return;
            }

            StopServer();

            _routerSocket?.Dispose();
            _poller?.Dispose();

            _disposed = true;
        }
        #endregion

        public HistoricalDataServer(int port, IHistoricalDataBroker broker)
        {
            if (broker == null) {
                throw new ArgumentNullException(nameof(broker), $"{nameof(broker)} cannot be null");
            }

            _listenPort = port;
            _broker = broker;
            _broker.HistoricalDataArrived += BrokerHistoricalDataArrived;
        }

        /// <summary>
        ///     Start the server.
        /// </summary>
        public void StartServer()
        {
            CheckDisposed();

            if (ServerRunning) {
                return;
            }

            _routerSocket = new RouterSocket($"tcp://*:{_listenPort}");
            _routerSocket.ReceiveReady += SocketReceiveReady;

            _poller = new NetMQPoller {_routerSocket};
            _poller.RunAsync();
        }

        /// <summary>
        ///     Stop the server.
        /// </summary>
        public void StopServer()
        {
            CheckDisposed();

            if (!ServerRunning) {
                return;
            }

            if (_poller != null && _poller.IsRunning) {
                _poller.Stop();
            }
        }

        #region Event handlers
        private void BrokerHistoricalDataArrived(object sender, HistoricalDataEventArgs e)
        {
            if (_disposed) {
                return;
            }

            try {
                _socketFree.Wait();

                SendFilledHistoricalRequest(e.Request, e.Data);
            }
            finally {
                _socketFree.Set();
            }
        }

        /// <summary>
        ///     This is called when a new historical data request or data push request is made.
        /// </summary>
        private void SocketReceiveReady(object sender, NetMQSocketEventArgs e)
        {
            if (_disposed) {
                return;
            }

            try {
                _socketFree.Wait();
                // Here we process the first two message parts: first, the identity string of the client
                var requesterIdentity = e.Socket.ReceiveFrameString();
                // Second: the string specifying the type of request
                var text = e.Socket.ReceiveFrameString();

                if (text.Equals("HISTREQ", StringComparison.InvariantCultureIgnoreCase)) // The client wants to request some data
                {
                    AcceptHistoricalDataRequest(requesterIdentity, e.Socket);
                }
                else if (text.Equals("HISTPUSH", StringComparison.InvariantCultureIgnoreCase)) // The client wants to push some data into the db
                {
                    AcceptDataAdditionRequest(requesterIdentity, e.Socket);
                }
                else if (text.Equals("AVAILABLEDATAREQ", StringComparison.InvariantCultureIgnoreCase)) // Client wants to know what kind of data we have stored locally
                {
                    AcceptAvailableDataRequest(requesterIdentity, e.Socket);
                }
                else {
                    _logger.Info($"Unrecognized request to historical data broker: {text}");
                }
            }
            finally {
                _socketFree.Set();
            }
        }
        #endregion

        /// <summary>
        ///     Given a historical data request and the data that fill it,
        ///     send the reply to the client who made the request.
        /// </summary>
        private void SendFilledHistoricalRequest(HistoricalDataRequest request, List<OHLCBar> data)
        {
            using (var ms = new MemoryStream()) {
                // This is a 5 part message
                // 1st message part: the identity string of the client that we're routing the data to
                var clientIdentity = request.RequesterIdentity;

                _routerSocket.SendMoreFrame(clientIdentity ?? string.Empty);
                // 2nd message part: the type of reply we're sending
                _routerSocket.SendMoreFrame("HISTREQREP");
                // 3rd message part: the HistoricalDataRequest object that was used to make the request
                _routerSocket.SendMoreFrame(MyUtils.ProtoBufSerialize(request, ms));
                // 4th message part: the size of the uncompressed, serialized data. Necessary for decompression on the client end.
                var uncompressed = MyUtils.ProtoBufSerialize(data, ms);

                _routerSocket.SendMoreFrame(BitConverter.GetBytes(uncompressed.Length));
                // 5th message part: the compressed serialized data.
                var compressed = LZ4Codec.EncodeHC(uncompressed, 0, uncompressed.Length); // compress

                _routerSocket.SendFrame(compressed);
            }
        }

        /// <summary>
        ///     Handles requests for information on data that is available in local storage
        /// </summary>
        private void AcceptAvailableDataRequest(string requesterIdentity, NetMQSocket socket)
        {
            using (var ms = new MemoryStream()) {
                // Get the instrument
                bool hasMore;
                var buffer = socket.ReceiveFrameBytes(out hasMore);
                var instrument = MyUtils.ProtoBufDeserialize<Instrument>(buffer, ms);

                _logger.Info($"Received local data storage info request for {instrument.Symbol}.");
                // And send the reply
                var storageInfo = _broker.GetAvailableDataInfo(instrument);

                socket.SendMoreFrame(requesterIdentity);
                socket.SendMoreFrame("AVAILABLEDATAREP");
                socket.SendMoreFrame(MyUtils.ProtoBufSerialize(instrument, ms));
                socket.SendMoreFrame(BitConverter.GetBytes(storageInfo.Count));

                foreach (var sdi in storageInfo) {
                    socket.SendMoreFrame(MyUtils.ProtoBufSerialize(sdi, ms));
                }
            }

            socket.SendFrame("END");
        }

        /// <summary>
        ///     Handles incoming data "push" requests: the client sends data for us to add to local storage.
        /// </summary>
        private void AcceptDataAdditionRequest(string requesterIdentity, NetMQSocket socket)
        {
            using (var ms = new MemoryStream()) {
                // Final message part: receive the DataAdditionRequest object
                bool hasMore;
                var buffer = socket.ReceiveFrameBytes(out hasMore);
                var request = MyUtils.ProtoBufDeserialize<DataAdditionRequest>(buffer, ms);

                _logger.Info($"Received data push request for {request.Instrument.Symbol}.");
                // Start building the reply
                socket.SendMoreFrame(requesterIdentity);
                socket.SendMoreFrame("PUSHREP");

                try {
                    _broker.AddData(request);

                    socket.SendFrame("OK");
                }
                catch (Exception ex) {
                    socket.SendMoreFrame("ERROR");
                    socket.SendFrame(ex.Message);
                }
            }
        }

        /// <summary>
        ///     Processes incoming historical data requests.
        /// </summary>
        private void AcceptHistoricalDataRequest(string requesterIdentity, NetMQSocket socket)
        {
            // Third: a serialized HistoricalDataRequest object which contains the details of the request
            bool hasMore;
            var buffer = socket.ReceiveFrameBytes(out hasMore);

            using (var ms = new MemoryStream()) {
                ms.Write(buffer, 0, buffer.Length);
                ms.Position = 0;

                var request = Serializer.Deserialize<HistoricalDataRequest>(ms);

                _logger.Info(
                    string.Format(
                        "Historical Data Request from client {0}: {7} {1} @ {2} from {3} to {4} Location: {5} {6:;;SaveToLocal}",
                        requesterIdentity,
                        request.Instrument.Symbol,
                        Enum.GetName(typeof(BarSize), request.Frequency),
                        request.StartingDate,
                        request.EndingDate,
                        request.DataLocation,
                        request.SaveDataToStorage ? 0 : 1,
                        request.Instrument.Datasource.Name));

                request.RequesterIdentity = requesterIdentity;

                try {
                    _broker.RequestHistoricalData(request);
                }
                catch (Exception ex) {
                    // There's some sort of problem with fulfilling the request. Inform the client.
                    SendErrorReply(requesterIdentity, request.RequestID, ex.Message);
                }
            }
        }

        /// <summary>
        ///     If a historical data request can't be filled,
        ///     this method sends a reply with the relevant error.
        /// </summary>
        private void SendErrorReply(string requesterIdentity, int requestId, string message)
        {
            // This is a 4 part message
            // 1st message part: the identity string of the client that we're routing the data to
            _routerSocket.SendMoreFrame(requesterIdentity);
            // 2nd message part: the type of reply we're sending
            _routerSocket.SendMoreFrame("ERROR");
            // 3rd message part: the request ID
            _routerSocket.SendMoreFrame(BitConverter.GetBytes(requestId));
            // 4th message part: the error
            _routerSocket.SendFrame(message);
        }

        private void CheckDisposed()
        {
            if (_disposed) {
                throw new ObjectDisposedException("HistoricalDataServer");
            }
        }
    }
}