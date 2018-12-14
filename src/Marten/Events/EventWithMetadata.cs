using System.Collections.Generic;

namespace Marten.Events
{
    public class EventWithMetadata
    {
        public object Event { get; set; }

        public Dictionary<string, object> MetaData { get; set; }
    }
}
