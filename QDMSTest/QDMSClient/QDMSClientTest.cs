// -----------------------------------------------------------------------
// <copyright file="QDMSClientTest.cs" company="">
// Copyright 2014 Alexander Soffronow Pagonidis
// </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Threading;

using Moq;

using NUnit.Framework;

using QDMS;

using QDMSServer;

namespace QDMSTest
{
    [TestFixture]
    // ReSharper disable once InconsistentNaming
    public class QDMSClientTest
    {
        private QDMSClient.QDMSClient _client;

        [SetUp]
        public void SetUp()
        {
            _client = new QDMSClient.QDMSClient("testingclient", "127.0.0.1", 5553, 5554, 5555, 5556);
        }

        [TearDown]
        public void TearDown()
        {
            _client.Dispose();
        }

        [Test]
        public void InstrumentAdditionRequestsAreSentCorrectly()
        {
            var instrumentSourceMock = new Mock<IInstrumentSource>();

            using (var instrumentsServer = new InstrumentsServer(5555, instrumentSourceMock.Object)) {
                instrumentsServer.StartServer();

                var rtdBrokerMock = new Mock<IRealTimeDataBroker>();
                using (var rtdServer = new RealTimeDataServer(5554, 5553, rtdBrokerMock.Object)) {
                    rtdServer.StartServer();

                    _client.Connect();

                    var exchange = new Exchange {ID = 1, Name = "NYSE", Sessions = new List<ExchangeSession>(), Timezone = "Eastern Standard Time"};
                    var datasource = new Datasource {ID = 1, Name = "Yahoo"};
                    var instrument = new Instrument
                    {
                        Symbol = "SPY",
                        UnderlyingSymbol = "SPY",
                        Type = InstrumentType.Stock,
                        Currency = "USD",
                        Exchange = exchange,
                        Datasource = datasource,
                        Multiplier = 1
                    };

                    instrumentSourceMock.Setup(x => x.AddInstrument(It.IsAny<Instrument>(), It.IsAny<bool>(), It.IsAny<bool>())).Returns(instrument);

                    var result = _client.AddInstrument(instrument);

                    Thread.Sleep(50);

                    Assert.IsTrue(result != null);

                    instrumentSourceMock.Verify(
                        x => x.AddInstrument(
                            It.Is<Instrument>(
                                y =>
                                    y.Symbol == "SPY" &&
                                    y.Exchange != null &&
                                    y.Exchange.Name == "NYSE" &&
                                    y.Datasource != null &&
                                    y.Datasource.Name == "Yahoo" &&
                                    y.Type == InstrumentType.Stock &&
                                    y.Currency == "USD" &&
                                    y.Multiplier == 1),
                            It.Is<bool>(y => y == false),
                            It.Is<bool>(y => y)));

                    rtdServer.StopServer();
                }

                instrumentsServer.StopServer();
            }
        }

        [Test]
        public void RequestHistoricalDataRaisesErrorEventAndReturnsMinusOneWhenDatesAreWrong()
        {
            var req = new HistoricalDataRequest
            {
                Instrument = new Instrument {ID = 1, Symbol = "SPY"},
                StartingDate = new DateTime(2012, 1, 1),
                EndingDate = new DateTime(2011, 1, 1),
                Frequency = BarSize.OneDay
            };

            var errorTriggered = false;
            _client.Error += (sender, e) => errorTriggered = true;

            Assert.AreEqual(-1, _client.RequestHistoricalData(req));

            Assert.IsTrue(errorTriggered);
        }

        [Test]
        public void RequestHistoricalDataRaisesErrorEventAndReturnsMinusOneWhenNotConnected()
        {
            var req = new HistoricalDataRequest
            {
                Instrument = new Instrument {ID = 1, Symbol = "SPY"},
                StartingDate = new DateTime(2012, 1, 1),
                EndingDate = new DateTime(2013, 1, 1),
                Frequency = BarSize.OneDay
            };

            var errorTriggered = false;
            _client.Error += (sender, e) => errorTriggered = true;

            Assert.AreEqual(-1, _client.RequestHistoricalData(req));

            Assert.IsTrue(errorTriggered);
        }

        [Test]
        public void RequestHistoricalDataRaisesErrorEventAndReturnsMinusOneWhenInstrumentIsNull()
        {
            var req = new HistoricalDataRequest
            {
                Instrument = null,
                StartingDate = new DateTime(2012, 1, 1),
                EndingDate = new DateTime(2013, 1, 1),
                Frequency = BarSize.OneDay
            };

            var errorTriggered = false;
            _client.Error += (sender, e) => errorTriggered = true;

            Assert.AreEqual(-1, _client.RequestHistoricalData(req));

            Assert.IsTrue(errorTriggered);
        }

        [Test]
        public void RequestRealTimelDataRaisesErrorEventAndReturnsMinusOneWhenInstrumentIsNull()
        {
            var req = new RealTimeDataRequest
            {
                Instrument = null
            };

            var errorTriggered = false;
            _client.Error += (sender, e) => errorTriggered = true;

            Assert.AreEqual(-1, _client.RequestRealTimeData(req));

            Assert.IsTrue(errorTriggered);
        }

        [Test]
        public void RequestRealTimelDataRaisesErrorEventAndReturnsMinusOneWhenNotConnected()
        {
            var req = new RealTimeDataRequest
            {
                Instrument = new Instrument {ID = 1, Symbol = "SPY"}
            };

            var errorTriggered = false;
            _client.Error += (sender, e) => errorTriggered = true;

            Assert.AreEqual(-1, _client.RequestRealTimeData(req));

            Assert.IsTrue(errorTriggered);
        }
    }
}