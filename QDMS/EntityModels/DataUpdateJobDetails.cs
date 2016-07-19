﻿// -----------------------------------------------------------------------
// <copyright file="DataUpdateJob.cs" company="">
// Copyright 2014 Alexander Soffronow Pagonidis
// </copyright>
// -----------------------------------------------------------------------

// Holds instructions on how a data update job is to be performed.

using System;
using System.ComponentModel;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Runtime.CompilerServices;

using QDMS.Annotations;

namespace QDMS
{
    public class DataUpdateJobDetails : INotifyPropertyChanged
    {
        private string _name;

        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int ID { get; set; }

        /// <summary>
        ///     Name.
        /// </summary>
        [MaxLength(255)]
        public string Name
        {
            get { return _name; }
            set
            {
                _name = value;

                OnPropertyChanged();
            }
        }

        /// <summary>
        ///     If true, all instruments with the given tag are matched. If false, a specific instrument is matched.
        /// </summary>
        public bool UseTag { get; set; }

        /// <summary>
        ///     If UseTag = false, this instrument's data gets updated.
        /// </summary>
        public int? InstrumentID { get; set; }

        /// <summary>
        ///     Instrument.
        /// </summary>
        public virtual Instrument Instrument { get; set; }

        /// <summary>
        ///     If UseTag = true, instruments having this tag are updated.
        /// </summary>
        public int? TagID { get; set; }

        /// <summary>
        ///     Tag.
        /// </summary>
        public virtual Tag Tag { get; set; }

        /// <summary>
        ///     If true, updates will only happen monday through friday.
        /// </summary>
        public bool WeekDaysOnly { get; set; }

        /// <summary>
        ///     The time when the job runs.
        /// </summary>
        public TimeSpan Time { get; set; }

        /// <summary>
        ///     The data frequency to be updated.
        /// </summary>
        public BarSize Frequency { get; set; }

        #region INotifyPropertyChanged implementation
        public event PropertyChangedEventHandler PropertyChanged;
        #endregion

        [NotifyPropertyChangedInvocator]
        protected virtual void OnPropertyChanged([CallerMemberName] string propertyName = null)
        {
            PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(propertyName));
        }
    }
}