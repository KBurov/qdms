// Imported from https://sentineliqf.codeplex.com

using System;

using QDMSServer.DataSources;

namespace IQFeed.ConsoleDemo
{
    public class MyAdmin : IQAdminSocketClient
    {
        public MyAdmin()
            : base(80) {}

        protected override void OnClientStatsEvent(ClientStatsEventArgs e)
        {
            base.OnClientStatsEvent(e);
            Console.WriteLine(
                "Client Id:" + e.clientId.ToString() +
                " Client Name:" + e.clientName +
                " Start Time:" + e.startTime.ToShortTimeString() +
                " KB in:" + e.kbSent.ToString() +
                " KB out:" + e.kbReceived.ToString() +
                " KB que:" + e.kbQueued.ToString() +
                " Port:" + e.type.ToString() +
                " Symbols Watched:" + e.symbolsWatched.ToString());
        }

        protected override void OnTextLineEvent(TextLineEventArgs e)
        {
            base.OnTextLineEvent(e);
        }
    }

    public class MyLookup : IQLookupHistorySymbolClient
    {
        public MyLookup()
            : base(80) {}

        protected override void OnTextLineEvent(TextLineEventArgs e)
        {
            base.OnTextLineEvent(e);
        }

        protected override void OnLookupEvent(LookupEventArgs e)
        {
            base.OnLookupEvent(e);
            if (e.Type == LookupType.REQ_HST_TCK) {
                if (e.Sequence == LookupSequence.MessageStart) {
                    Console.WriteLine("Start(" + e.Id + ")");
                }
                if (e.Sequence == LookupSequence.MessageEnd) {
                    Console.WriteLine("End(" + e.Id + ")");
                }
                if (e.Sequence == LookupSequence.MessageDetail) {
                    LookupTickEventArgs tea = e as LookupTickEventArgs;
                    Console.WriteLine(
                        "id:" + e.Id + "  Time:" + tea.DateTimeStamp.ToString() + " Lst:" + tea.Last.ToString() +
                        " LstSz:" + tea.LastSize.ToString() + " tv:" + tea.TotalVolume.ToString() + " Bid:" + tea.Bid.ToString() +
                        " Ask:" + tea.Ask.ToString() + " Bss:" + tea.Basis.ToString() + " tid:" + tea.TickId.ToString());
                }
            }
            if (e.Type == LookupType.REQ_HST_INT) {
                if (e.Sequence == LookupSequence.MessageStart) {
                    Console.WriteLine("Start(" + e.Id + ")");
                }
                if (e.Sequence == LookupSequence.MessageEnd) {
                    Console.WriteLine("End(" + e.Id + ")");
                }
                if (e.Sequence == LookupSequence.MessageDetail) {
                    LookupIntervalEventArgs tea = e as LookupIntervalEventArgs;
                    Console.WriteLine(
                        "id:" + e.Id + "  Time:" + tea.DateTimeStamp.ToString() + " Op:" + tea.Open.ToString() +
                        " Hi:" + tea.High.ToString() + " Lo:" + tea.Low.ToString() + " Cl:" + tea.Close.ToString() +
                        " Pvk:" + tea.PeriodVolume.ToString() + " Tv:" + tea.TotalVolume.ToString());
                }
            }
            if (e.Type == LookupType.REQ_HST_DWM) {
                if (e.Sequence == LookupSequence.MessageStart) {
                    Console.WriteLine("Start(" + e.Id + ")");
                }
                if (e.Sequence == LookupSequence.MessageEnd) {
                    Console.WriteLine("End(" + e.Id + ")");
                }
                if (e.Sequence == LookupSequence.MessageDetail) {
                    LookupDayWeekMonthEventArgs tea = e as LookupDayWeekMonthEventArgs;
                    Console.WriteLine(
                        "id:" + e.Id + "  Time:" + tea.DateTimeStamp.ToString() + " Op:" + tea.Open.ToString() +
                        " Hi:" + tea.High.ToString() + " Lo:" + tea.Low.ToString() + " Cl:" + tea.Close.ToString() +
                        " Pvk:" + tea.PeriodVolume.ToString() + " Oi:" + tea.OpenInterest.ToString());
                }
            }
            if (e.Type == LookupType.REQ_SYM_NAC) {
                if (e.Sequence == LookupSequence.MessageStart) {
                    Console.WriteLine("Start(" + e.Id + ")");
                    return;
                }
                if (e.Sequence == LookupSequence.MessageEnd) {
                    Console.WriteLine("End(" + e.Id + ")");
                    return;
                }

                LookupNaicSymbolEventArgs lt = e as LookupNaicSymbolEventArgs;
                Console.WriteLine(lt.Naic + ", " + lt.Symbol + ", " + lt.Description);
                return;
            }
            if (e.Type == LookupType.REQ_SYM_SIC) {
                if (e.Sequence == LookupSequence.MessageStart) {
                    Console.WriteLine("Start(" + e.Id + ")");
                    return;
                }
                if (e.Sequence == LookupSequence.MessageEnd) {
                    Console.WriteLine("End(" + e.Id + ")");
                    return;
                }

                LookupSicSymbolEventArgs lt = e as LookupSicSymbolEventArgs;
                Console.WriteLine(lt.Sic + ", " + lt.Symbol + ", " + lt.Description);
                return;
            }
            if (e.Type == LookupType.REQ_SYM_SYM) {
                if (e.Sequence == LookupSequence.MessageStart) {
                    Console.WriteLine("Start(" + e.Id + ")");
                    return;
                }
                if (e.Sequence == LookupSequence.MessageEnd) {
                    Console.WriteLine("End(" + e.Id + ")");
                    return;
                }

                LookupSymbolEventArgs lt = e as LookupSymbolEventArgs;
                Console.WriteLine(lt.Symbol + ", " + lt.MarketId.ToString() + ", " + lt.Description);
                return;
            }

        }
    }

    public class MyTableLookup : IQLookupTableClient
    {
        public MyTableLookup()
            : base(80) {}

        protected override void OnTextLineEvent(TextLineEventArgs e)
        {
            base.OnTextLineEvent(e);
        }

        protected override void OnLookupEvent(LookupEventArgs e)
        {
            base.OnLookupEvent(e);
            if (e.Sequence == LookupSequence.MessageStart) {
                Console.WriteLine("*** Start(" + e.Type.ToString() + ")");
                return;
            }

            if (e.Sequence == LookupSequence.MessageDetail) {
                if (e.Type == LookupType.REQ_TAB_MKT) {
                    LookupTableMarketEventArgs lt = e as LookupTableMarketEventArgs;
                    Console.WriteLine(lt.Code.ToString() + ", " + lt.ShortName + ", " + lt.LongName);
                    return;
                }
                if (e.Type == LookupType.REQ_TAB_MKC) {
                    LookupTableMarketCenterEventArgs lt = e as LookupTableMarketCenterEventArgs;
                    Console.WriteLine(lt.Code.ToString() + ", (" + Util.ArrayToString(lt.MarketEquityId, ' ') + "), (" + Util.ArrayToString(lt.MarketOptionId, ' ') + ")");
                    return;
                }
                if (e.Type == LookupType.REQ_TAB_NAC) {
                    LookupTableNaicEventArgs lt = e as LookupTableNaicEventArgs;
                    Console.WriteLine(lt.Code.ToString() + ", " + lt.Description);
                    return;
                }
                if (e.Type == LookupType.REQ_TAB_SEC) {
                    LookupTableSecurityTypeEventArgs lt = e as LookupTableSecurityTypeEventArgs;
                    Console.WriteLine(lt.Code.ToString() + ", " + lt.ShortName + ", " + lt.LongName);
                    return;
                }
                if (e.Type == LookupType.REQ_TAB_SIC) {
                    LookupTableSicEventArgs lt = e as LookupTableSicEventArgs;
                    Console.WriteLine(lt.Code.ToString() + ", " + lt.Description);
                    return;
                }
            }

            if (e.Sequence == LookupSequence.MessageEnd) {
                Console.WriteLine("*** End (" + e.Type.ToString() + ")");
                return;
            }
        }
    }

    public class MyLevel1 : IQLevel1Client
    {
        public MyLevel1()
            : base(80) {}

        protected override void OnTextLineEvent(TextLineEventArgs e)
        {
            base.OnTextLineEvent(e);
        }

        protected override void OnLevel1SummaryUpdateEvent(Level1SummaryUpdateEventArgs e)
        {
            base.OnLevel1SummaryUpdateEvent(e);
            Console.WriteLine("Summary:" + e.Summary.ToString() + " " + e.Symbol + " " + e.Last.ToString());
        }

        protected override void OnLevel1FundamentalEvent(Level1FundamentalEventArgs e)
        {
            base.OnLevel1FundamentalEvent(e);
            Console.WriteLine("Fund: " + e.Symbol + " 52wh:" + e.High52Week.ToString() + " 52wl" + e.Low52Week.ToString());
        }

        protected override void OnLevel1TimerEvent(Level1TimerEventArgs e)
        {
            base.OnLevel1TimerEvent(e);
            Console.WriteLine("Timer: " + e.DateTimeStamp.ToLongTimeString());
        }

        protected override void OnLevel1NewsEvent(Level1NewsEventArgs e)
        {
            base.OnLevel1NewsEvent(e);
            Console.WriteLine("News: " + e.Headline);
        }

        protected override void OnLevel1RegionalEvent(Level1RegionalEventArgs e)
        {
            base.OnLevel1RegionalEvent(e);
            Console.WriteLine("Regional: " + e.Symbol + " ask:" + e.RegionalAsk.ToString());
        }
    }

    class Program
    {
        static void Main()
        {
            IQConnect iqc = new IQConnect("VICTOR_DIAMOND_6086", "1.0");
            iqc.launch();

            // Initialise one admin port
            var admin = new MyAdmin();
            admin.Connect();
            admin.SetClientStats(true);
            admin.SetClientName("Admin");

            // Initialise lookup socket
            var lookup = new MyLookup();
            lookup.Connect();
            lookup.SetClientName("Lookup");
            lookup.RequestTickData("ORCL", 100, false);

            Console.ReadKey();

            lookup.Disconnect();
            admin.Disconnect();
        }
    }
}