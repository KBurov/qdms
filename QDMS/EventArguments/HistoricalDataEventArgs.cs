// -----------------------------------------------------------------------
// <copyright file="HistoricalDataEventArgs.cs" company="">
// Copyright 2013 Alexander Soffronow Pagonidis
// </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;

namespace QDMS
{
    public class HistoricalDataEventArgs : EventArgs
    {
        /// <summary>
        ///     The request that is being filled.
        /// </summary>
        public HistoricalDataRequest Request { get; }

        /// <summary>
        ///     The data.
        /// </summary>
        public List<OHLCBar> Data { get; }

        /// <summary>
        ///     Parameterless constructor is needed for protobuf-net to properly serialize this object.
        /// </summary>
        private HistoricalDataEventArgs() {}

        /// <summary>
        ///     Historical data event args.
        /// </summary>
        public HistoricalDataEventArgs(HistoricalDataRequest request, List<OHLCBar> data)
        {
            Request = request;
            Data = data;
        }
    }
}