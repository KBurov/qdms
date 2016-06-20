﻿// Imported from https://sentineliqf.codeplex.com

using System;

namespace QDMSServer.DataSources
{
    public class Time
    {
        public Time(int hour = 0, int minute = 0, int second = 0)
        {
            _hour = hour;
            _minute = minute;
            _second = second;
        }

        public Time(string textTime)
        {
            if (textTime.Length < 8) {
                _hour = 0;
                _minute = 0;
                _second = 0;
                return;
            }
            if (!int.TryParse(textTime.Substring(0, 2), out _hour)) _hour = 0;
            if (!int.TryParse(textTime.Substring(3, 2), out _minute)) _minute = 0;
            if (!int.TryParse(textTime.Substring(6, 2), out _second)) _second = 0;
        }

        public int Hour { get { return _hour; } set { _hour = value; } }
        public int Minute { get { return _minute; } set { _minute = value; } }
        public int Second { get { return _second; } set { _second = value; } }
        public string IQFeedFormat { get { return string.Format("{0}{1}{2}", _hour.ToString("00"), _minute.ToString("00"), _second.ToString("00")); } }

        #region private
        private int _hour;
        private int _minute;
        private int _second;
        #endregion
    }

    public enum PeriodType
    {
        Second,
        Minute,
        Hour
    }

    public class Interval
    {
        public Interval(PeriodType periodType, int periods)
        {
            _periodType = periodType;
            _periods = periods;
        }

        public PeriodType PeriodType { get { return _periodType; } }
        public int Periods { get { return _periods; } }

        public int Seconds
        {
            get
            {
                switch (_periodType) {
                    case PeriodType.Second:
                        return _periods;
                    case PeriodType.Minute:
                        return _periods * 60;
                    case PeriodType.Hour:
                        return _periods * 3600;
                }
                throw new Exception("May not get seconds for " + _periodType.ToString());
            }
        }

        #region private
        private PeriodType _periodType;
        private int _periods;
        #endregion
    }
}