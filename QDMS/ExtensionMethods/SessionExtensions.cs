// -----------------------------------------------------------------------
// <copyright file="ISessionExtensions.cs" company="">
// Copyright 2014 Alexander Soffronow Pagonidis
// </copyright>
// -----------------------------------------------------------------------

using System;
using System.Globalization;

namespace QDMS
{
    public static class SessionExtensions
    {
        public static InstrumentSession ToInstrumentSession(this ISession session)
        {
            return new InstrumentSession
            {
                OpeningDay = session.OpeningDay,
                OpeningTime = TimeSpan.FromSeconds(session.OpeningTime.TotalSeconds),
                ClosingDay = session.ClosingDay,
                ClosingTime = TimeSpan.FromSeconds(session.ClosingTime.TotalSeconds),
                IsSessionEnd = session.IsSessionEnd
            };
        }

        public static bool Overlaps(this ISession session1, ISession session2)
        {
            //Create starting and ending DTs for the sessions
            var arbitraryStartPoint = new DateTime(2014, 1, 1, 0, 0, 0, 0, new GregorianCalendar(), DateTimeKind.Utc);
            DateTime session1Start, session1End, session2StartBack, session2StartForward, session2EndBack, session2EndForward;

            SessionToDTs(session1, arbitraryStartPoint, out session1Start, out session1End);
            //to make sure all overlap scenarios are covered, the 2nd session is done both backwards and forwards
            SessionToDTs(session2, session1Start, out session2StartForward, out session2EndForward);
            SessionToDTs(session2, session1Start, out session2StartBack, out session2EndBack, false);

            return DateTimePeriodsOverlap(session1Start, session1End, session2StartBack, session2EndBack)
                   || DateTimePeriodsOverlap(session1Start, session1End, session2StartForward, session2EndForward);
        }

        private static bool DateTimePeriodsOverlap(DateTime p1start, DateTime p1end, DateTime p2start, DateTime p2end)
        {
            //engulfing
            if (p1start > p2start && p1end < p2end) {
                return true;
            }
            if (p2start > p1start && p2end < p1end) {
                return true;
            }
            //partial overlap
            if (p1start < p2end && p1end > p2end) {
                return true;
            }
            if (p2start < p1end && p2end > p1end) {
                return true;
            }

            return false;
        }

        private static void SessionToDTs(ISession session, DateTime startingPoint, out DateTime start, out DateTime end, bool forwards = true)
        {
            start = startingPoint;

            while (start.DayOfWeek.ToInt() != (int) session.OpeningDay) {
                start = start.AddDays(forwards ? 1 : -1);
            }

            end = start;

            while (end.DayOfWeek.ToInt() != (int) session.ClosingDay) {
                end = end.AddDays(1);
            }

            start = start.Date + session.OpeningTime;
            end = end.Date + session.ClosingTime;
        }
    }
}