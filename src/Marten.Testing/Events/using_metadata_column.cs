using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Baseline;
using Marten.Events;
using Marten.Testing.Events.Projections;
using Shouldly;
using Xunit;

namespace Marten.Testing.Events
{
    public class using_metadata_column
    {
        [Fact]
        public async Task insert_event_with_metadata()
        {
            var store = InitStore();

            using (var session = store.OpenSession())
            {
                var joined = new MembersJoined { Members = new[] { "Rand", "Matt", "Perrin", "Thom" } };

                var id = session.Events.StartStream(Guid.NewGuid(), new EventWithMetadata
                {
                    Event = joined,
                    MetaData = new Dictionary<string, object>()
                    {
                        ["property"] = "value"
                    }
                }).Id;
                session.SaveChanges();

                var streamEvents = await session.Events.FetchStreamAsync(id);

                streamEvents.Count().ShouldBe(1);
                streamEvents.ElementAt(0).Data.ShouldBeOfType<MembersJoined>();
                streamEvents.ElementAt(0).MetaData["property"].ShouldBe("value");
            }
        }
        
        [Fact]
        public async Task start_stream_with_aggregateType_and_metadata()
        {
            var store = InitStore();

            using (var session = store.OpenSession())
            {
                var joined = new MembersJoined { Members = new[] { "Rand", "Matt", "Perrin", "Thom" } };

                var id = session.Events.StartStream<QuestParty>(Guid.NewGuid(), new EventWithMetadata
                {
                    Event = joined,
                    MetaData = new Dictionary<string, object>
                    {
                        ["property"] = "value"
                    }
                }).Id;
                session.SaveChanges();

                var streamEvents = await session.Events.FetchStreamAsync(id);

                streamEvents.Count.ShouldBe(1);
                streamEvents.ElementAt(0).Data.ShouldBeOfType<MembersJoined>();
                streamEvents.ElementAt(0).MetaData["property"].ShouldBe("value");
            }
        }
        
        [Fact]
        public async Task start_stream_with_aggregateType_and_metadata_string_id()
        {
            var store = InitStore(true, StreamIdentity.AsString);

            using (var session = store.OpenSession())
            {
                var joined = new MembersJoined { Members = new[] { "Rand", "Matt", "Perrin", "Thom" } };

                var id = session.Events.StartStream<QuestParty>(Guid.NewGuid().ToString(), new EventWithMetadata
                {
                    Event = joined,
                    MetaData = new Dictionary<string, object>
                    {
                        ["property"] = "value"
                    }
                }).Key;
                session.SaveChanges();

                var streamEvents = await session.Events.FetchStreamAsync(id.ToString());

                streamEvents.Count.ShouldBe(1);
                streamEvents.ElementAt(0).Data.ShouldBeOfType<MembersJoined>();
                streamEvents.ElementAt(0).MetaData["property"].ShouldBe("value");
            }
        }

        [Fact]
        public async Task append_event_with_metadata()
        {
            var store = InitStore();
            
            var streamId = Guid.NewGuid();
            
            using (var session = store.OpenSession())
            {
                var joined = new MembersJoined { Members = new[] { "Rand", "Matt", "Perrin", "Thom" } };

                session.Events.StartStream(streamId, joined);
                session.SaveChanges();
            }

            using (var session = store.OpenSession())
            {
                var joined = new MembersJoined { Members = new[] { "Rand", "Matt", "Perrin", "Thom" } };

                var id = session.Events.Append(streamId, new EventWithMetadata
                {
                    Event = joined,
                    MetaData = new Dictionary<string, object>()
                    {
                        ["property"] = "value"
                    }
                }).Id;
                session.SaveChanges();

                var streamEvents = await session.Events.FetchStreamAsync(id);

                streamEvents.Count.ShouldBe(2);
                streamEvents.ElementAt(1).Data.ShouldBeOfType<MembersJoined>();
                streamEvents.ElementAt(1).MetaData["property"].ShouldBe("value");
            }
        }

        [Fact]
        public async Task insert_event_with_metadata_complex_object()
        {
            var store = InitStore();

            using (var session = store.OpenSession())
            {
                var joined = new MembersJoined { Members = new[] { "Rand", "Matt", "Perrin", "Thom" } };

                var id = session.Events.StartStream(Guid.NewGuid(), new EventWithMetadata
                {
                    Event = joined,
                    MetaData = new Dictionary<string, object>()
                    {
                        ["version"] = new ComplexMetadataObject { Name = "42" }
                    }
                }).Id;
                session.SaveChanges();

                var streamEvents = await session.Events.FetchStreamAsync(id);

                streamEvents.Count.ShouldBe(1);
                streamEvents.ElementAt(0).Data.ShouldBeOfType<MembersJoined>();
                streamEvents.ElementAt(0).MetaData["version"].As<ComplexMetadataObject>().Name.ShouldBe("42");
            }
        }

        class ComplexMetadataObject
        {
            public string Name { get; set; }
        }

        private static DocumentStore InitStore(bool cleanSchema = true, StreamIdentity streamIdentity = StreamIdentity.AsGuid)
        {
            var databaseSchema = $"using_metadata_column";

            var store = DocumentStore.For(_ =>
            {
                _.Events.DatabaseSchemaName = databaseSchema;

                _.AutoCreateSchemaObjects = AutoCreate.All;

                _.Connection(ConnectionSource.ConnectionString);

                _.Events.InlineProjections.AggregateStreamsWith<QuestParty>();

                _.Events.AddEventType(typeof(MembersJoined));
                _.Events.AddEventType(typeof(MembersDeparted));
                _.Events.AddEventType(typeof(QuestStarted));

                _.Events.StreamIdentity = streamIdentity;
            });

            if (cleanSchema)
            {
                store.Advanced.Clean.CompletelyRemoveAll();
            }

            return store;
        }
    }
}