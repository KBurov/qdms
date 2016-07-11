// -----------------------------------------------------------------------
// <copyright file="Program.cs" company="">
// Copyright 2013 Alexander Soffronow Pagonidis
// </copyright>
// -----------------------------------------------------------------------

using System;
using System.Linq;
using System.Threading;

using QDMS;

namespace SampleApp
{
    internal class Program
    {
        private static void Main()
        {
            //create the client, assuming the default port settings
            using (var client = new QDMSClient.QDMSClient("SampleClient", "127.0.0.1", 5556, 5557, 5558, 5555)) {
                //hook up the events needed to receive data & error messages
                client.HistoricalDataReceived += client_HistoricalDataReceived;
                client.RealTimeDataReceived += client_RealTimeDataReceived;
                client.LocallyAvailableDataInfoReceived += client_LocallyAvailableDataInfoReceived;
                client.Error += client_Error;
                //connect to the server
                client.Connect();
                //make sure the connection was succesful before we continue
                if (!client.Connected) {
                    Console.WriteLine("Could not connect.");
                    Console.WriteLine("Press enter to exit.");
                    Console.ReadLine();
                    return;
                }
                //request the list of available instruments
                var instruments = client.FindInstruments();

                foreach (var i in instruments) {
                    Console.WriteLine($"Instrument ID {i.ID}: {i.Symbol} ({i.Type}), Datasource: {i.Datasource.Name}");
                }

                Thread.Sleep(3000);
                //then we grab some historical data from Yahoo
                //start by finding the SPY instrument
                var spy = instruments.FirstOrDefault(x => x.Symbol == "SPY" && x.Datasource.Name == "Yahoo");
                if (spy != null) {
                    var req = new HistoricalDataRequest(
                        spy,
                        BarSize.OneDay,
                        new DateTime(2013, 1, 1),
                        new DateTime(2013, 1, 15));

                    client.RequestHistoricalData(req);

                    Thread.Sleep(3000);
                    //now that we downloaded the data, let's make a request to see what is stored locally
                    client.GetLocallyAvailableDataInfo(spy);

                    Thread.Sleep(3000);
                    //finally send a real time data request (from the simulated data datasource)
                    spy.Datasource.Name = "SIM";
                    var rtReq = new RealTimeDataRequest(spy, BarSize.OneSecond);
                    client.RequestRealTimeData(rtReq);

                    Thread.Sleep(3000);
                    //And then cancel the real time data stream
                    client.CancelRealTimeData(spy);
                }

                Console.WriteLine("Press enter to exit.");
                Console.ReadLine();
            }
        }

        private static void client_LocallyAvailableDataInfoReceived(object sender, LocallyAvailableDataInfoReceivedEventArgs e)
        {
            foreach (var s in e.StorageInfo) {
                Console.WriteLine($"Freq: {s.Frequency} - From {s.EarliestDate} to {s.LatestDate}");
            }
        }

        private static void client_Error(object sender, ErrorArgs e)
        {
            Console.WriteLine($"Error {e.ErrorCode}: {e.ErrorMessage}");
        }

        private static void client_RealTimeDataReceived(object sender, RealTimeDataEventArgs e)
        {
            Console.WriteLine($"Real Time Data Received: O: {e.Open}  H: {e.High}  L: {e.Low}  C: {e.Close}");
        }

        private static void client_HistoricalDataReceived(object sender, HistoricalDataEventArgs e)
        {
            Console.WriteLine("Historical data received:");
            foreach (var bar in e.Data) {
                Console.WriteLine($"{bar.DT} - O: {bar.Open}  H: {bar.High}  L: {bar.Low}  C: {bar.Close}");
            }
        }
    }
}