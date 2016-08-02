// -----------------------------------------------------------------------
// <copyright file="UnderlyingSymbol.cs" company="">
// Copyright 2013 Alexander Soffronow Pagonidis
// </copyright>
// -----------------------------------------------------------------------

//In the future this class can hold stuff like margin requirements as well.

using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.IO;

using ProtoBuf;

namespace QDMS
{
    [ProtoContract]
    [Serializable]
    public class UnderlyingSymbol : ICloneable
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        [ProtoMember(1)]
        public int ID { get; set; }

        [ProtoMember(2)]
        [MaxLength(255)]
        public string Symbol { get; set; }

        // The byte is what we save to the database, the ExpirationRule is what we use in our applications
        public byte[] ExpirationRule
        {
            get { return MyUtils.ProtoBufSerialize(Rule, new MemoryStream()); }
            set { Rule = MyUtils.ProtoBufDeserialize<ExpirationRule>(value, new MemoryStream()); }
        }

        [NotMapped]
        [ProtoMember(3)]
        public ExpirationRule Rule { get; set; }

        public DateTime ExpirationDate(int year, int month, string countryCode = "US")
        {
            var referenceDay = new DateTime(year, month, 1);
            var calendar = MyUtils.GetCalendarFromCountryCode(countryCode);
            int day;

            referenceDay = referenceDay.AddMonths((int) Rule.ReferenceRelativeMonth);

            if (Rule.ReferenceDayIsLastBusinessDayOfMonth) {
                var tmpDay = referenceDay.AddMonths(1).AddDays(-1);

                while (!calendar.isBusinessDay(tmpDay)) {
                    tmpDay = tmpDay.AddDays(-1);
                }

                day = tmpDay.Day;
            }
            else if (Rule.ReferenceUsesDays) // we use a fixed number of days from the start of the month
            {
                day = Rule.ReferenceDays;
            }
            else // we use a number of weeks and then a weekday of that week
            {
                if (Rule.ReferenceWeekDayCount == WeekDayCount.Last) //the last week of the month
                {
                    var tmpDay = referenceDay.AddMonths(1).AddDays(-1);

                    while (tmpDay.DayOfWeek.ToInt() != (int) Rule.ReferenceWeekDay) {
                        tmpDay = tmpDay.AddDays(-1);
                    }

                    day = tmpDay.Day;
                }
                else // 1st to 4th week of the month, just loop until we find the right day
                {
                    var weekCount = 0;

                    while (weekCount < (int) Rule.ReferenceWeekDayCount + 1) {
                        if (referenceDay.DayOfWeek.ToInt() == (int) Rule.ReferenceWeekDay)
                            weekCount++;

                        referenceDay = referenceDay.AddDays(1);
                    }

                    day = referenceDay.Day - 1;
                }
            }

            referenceDay = new DateTime(year, month, day).AddMonths((int) Rule.ReferenceRelativeMonth);

            if (Rule.ReferenceDayMustBeBusinessDay) {
                while (!calendar.isBusinessDay(referenceDay)) {
                    referenceDay = referenceDay.AddDays(-1);
                }
            }

            switch (Rule.DayType) {
                case DayType.Business:
                    var daysLeft = Rule.DaysBefore;
                    var daysBack = 0;

                    while (daysLeft > 0) {
                        daysBack++;

                        if (calendar.isBusinessDay(referenceDay.AddDays(-daysBack)))
                            daysLeft--;
                    }

                    return referenceDay.AddDays(-daysBack);
                case DayType.Calendar:
                    return referenceDay.AddDays(-Rule.DaysBefore);
                default:
                    return referenceDay;
            }
        }

        #region ICloneable implementation
        /// <summary>
        ///     Creates a new object that is a copy of the current instance.
        /// </summary>
        /// <returns>
        ///     A new object that is a copy of this instance.
        /// </returns>
        public object Clone()
        {
            return new UnderlyingSymbol
            {
                ID = ID,
                ExpirationRule = ExpirationRule,
                Symbol = Symbol
            };
        }
        #endregion

        public override string ToString()
        {
            return Symbol;
        }
    }
}