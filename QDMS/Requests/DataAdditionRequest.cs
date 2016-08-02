// -----------------------------------------------------------------------
// <copyright file="DataAdditionRequest.cs" company="">
// Copyright 2013 Alexander Soffronow Pagonidis
// </copyright>
// -----------------------------------------------------------------------

using System.Collections.Generic;

using ProtoBuf;

namespace QDMS
{
    [ProtoContract]
    public class DataAdditionRequest
    {
        [ProtoMember(1)]
        public BarSize Frequency { get; set; }

        [ProtoMember(2)]
        public Instrument Instrument { get; set; }

        [ProtoMember(3)]
        public List<OHLCBar> Data { get; set; }

        [ProtoMember(4)]
        public bool Overwrite { get; set; }

        /// <summary>
        ///     If set to true, all adjusted values will be re-calculated
        /// </summary>
        public bool AdjustData { get; set; }

        public DataAdditionRequest()
        {
            Data = new List<OHLCBar>();
        }

        public DataAdditionRequest(BarSize frequency, Instrument instrument, List<OHLCBar> data, bool overwrite = true, bool adjust = false)
        {
            Data = data;
            Frequency = frequency;
            Instrument = instrument;
            Overwrite = overwrite;
            AdjustData = true;
        }

        public override string ToString()
        {
            return $"{Data.Count} bars @ {Frequency}, instrument: {Instrument}. {(Overwrite ? "Overwrite" : "")} {(AdjustData ? "Adjust" : "")}";
        }
    }
}