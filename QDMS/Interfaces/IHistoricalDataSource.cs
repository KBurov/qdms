﻿// -----------------------------------------------------------------------
// <copyright file="IHistoricalDataSource.cs" company="">
// Copyright 2013 Alexander Soffronow Pagonidis
// </copyright>
// -----------------------------------------------------------------------

using System;
using System.ComponentModel;

namespace QDMS
{
    public interface IHistoricalDataSource : INotifyPropertyChanged
    {
        /// <summary>
        ///     Whether the connection to the data source is up or not.
        /// </summary>
        bool Connected { get; }

        /// <summary>
        ///     The name of the data source.
        /// </summary>
        string Name { get; }

        /// <summary>
        ///     Connect to the data source.
        /// </summary>
        void Connect();

        /// <summary>
        ///     Disconnect from the data source.
        /// </summary>
        void Disconnect();

        void RequestHistoricalData(HistoricalDataRequest request);

        event EventHandler<HistoricalDataEventArgs> HistoricalDataArrived;

        /// <summary>
        ///     Fires on any error.
        /// </summary>
        event EventHandler<ErrorArgs> Error;

        /// <summary>
        ///     Fires on disconnection from the data source.
        /// </summary>
        event EventHandler<DataSourceDisconnectEventArgs> Disconnected;
    }
}