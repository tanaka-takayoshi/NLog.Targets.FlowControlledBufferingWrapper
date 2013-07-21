using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Threading;
using NLog.Common;
using NLog.Targets.Wrappers;

namespace NLog.Targets.FlowControlledBufferingWrapper
{
    [Target("FlowControlledBufferingWrapper", IsWrapper = true)]
    public class FlowControlledBufferingTargetWrapper : WrapperTargetBase
    {
        private LogEventInfoBuffer buffer;
        private Timer flushTimer;
        private Timer flowTimer;
        private bool overFlowed;
        private int flowCounter;

        [DefaultValue(100)]
        public int BufferSize { get; set; }

        [DefaultValue(-1)]
        public int FlushTimeout { get; set; }

        [DefaultValue(true)]
        public bool SlidingTimeout { get; set; }

        [DefaultValue(60000)]
        public int FlowTimeout { get; set; }

        [DefaultValue(100)]
        public int FlowCapacity { get; set; }

        protected override void InitializeTarget()
        {
            base.InitializeTarget();
            buffer = new LogEventInfoBuffer(BufferSize, false, 0);
            flushTimer = new Timer(FlushCallback, null, -1, -1);
            flowTimer = new Timer(FlowCallBack, null, -1, -1);
            overFlowed = FlowTimeout <= 0;
        }

        protected override void CloseTarget()
        {
            base.CloseTarget();
            if (flushTimer != null)
            {
                flushTimer.Dispose();
                flushTimer = null;
            }
            if (flowTimer != null)
            {
                flowTimer.Dispose();
                flowTimer = null;
            }
        }

        protected override void FlushAsync(AsyncContinuation asyncContinuation)
        {
            var events = buffer.GetEventsAndClear();

            if (!events.Any())
            {
                WrappedTarget.Flush(asyncContinuation);
            }
            else
            {
                WriteAsyncLogEvents(events, ex => WrappedTarget.Flush(asyncContinuation));
            }
        }

        protected void WriteAsyncLogEvents(AsyncLogEventInfo[] logEventInfos, AsyncContinuation continuation)
        {
          if (logEventInfos.Length == 0)
          {
            continuation(null);
          }
          else
          {
            var asyncLogEventInfoArray = new AsyncLogEventInfo[logEventInfos.Length];
            var remaining = logEventInfos.Length;
            for (var index = 0; index < logEventInfos.Length; ++index)
            {
              var originalContinuation = logEventInfos[index].Continuation;
              var asyncContinuation = (AsyncContinuation) (ex =>
              {
                originalContinuation(ex);
                if (Interlocked.Decrement(ref remaining) != 0)
                  return;
                continuation(null);
              });
              asyncLogEventInfoArray[index] = logEventInfos[index].LogEvent.WithContinuation(asyncContinuation);
            }
            WriteAsyncLogEvents(asyncLogEventInfoArray);
          }
        }

        protected override void Write(AsyncLogEventInfo logEvent)
        {
            this.WrappedTarget.PrecalculateVolatileLayouts(logEvent.LogEvent);

            if (FlowTimeout > 0)
            {
                if (++flowCounter >= FlowCapacity)
                {
                    overFlowed = true;
                }
                if (flowCounter == 1)
                {
                    flowTimer.Change(FlowTimeout, -1);
                }
            }

            var count = buffer.Append(logEvent);
            if (!overFlowed || count >= BufferSize)
            {
                var events = buffer.GetEventsAndClear();
                WrappedTarget.WriteAsyncLogEvents(events);
            }
            else
            {
                if (FlushTimeout > 0)
                {
                    // reset the timer on first item added to the buffer or whenever SlidingTimeout is set to true
                    if (SlidingTimeout || count == 1)
                    {
                        flushTimer.Change(FlushTimeout, -1);
                    }
                }
            }
        }

        private void FlushCallback(object state)
        {
            lock (SyncRoot)
            {
                if (!IsInitialized) return;

                var events = buffer.GetEventsAndClear();
                if (events.Any())
                {
                    WrappedTarget.WriteAsyncLogEvents(events);
                }
            }
        }

        private void FlowCallBack(object state)
        {
            lock (SyncRoot)
            {
                if (!IsInitialized) return;

                flowCounter = 0;
            }
        }
    }
}
