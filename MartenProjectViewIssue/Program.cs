using System;
using System.Linq;
using Baseline.Dates;
using FluentAssertions;
using Marten;
using Marten.Events.Projections.Async;

namespace MartenProjectViewIssue
{
    class Program
    {
        static void Main(string[] args)
        {
            var store = DocumentStore.For(_ =>
            {
                _.Connection("Server=127.0.0.1;Port=5432;Database=marten;User Id=postgres;Password=postgres;");
                _.AutoCreateSchemaObjects = AutoCreate.All;
                _.Events.InlineProjections.AggregateStreamsWith<User>();
                _.Events.ProjectView<User, Guid>().DeleteEvent<UserDeleted>();
            });

            var userId = Guid.NewGuid();

            using (var session = store.OpenSession())
            {
                session.Events.Append(userId, new UserCreated {Id = userId, Name = "Stu"});
                session.SaveChanges();

                session.Query<User>().Count().Should().Be(1);

                session.Events.Append(userId, new UserDeleted {Id = userId});
                session.SaveChanges();

                session.Query<User>().Count().Should().Be(0);

                var settings = new DaemonSettings
                {
                    LeadingEdgeBuffer = 0.Seconds()
                };

                using (var daemon = store.BuildProjectionDaemon(new[] {typeof(User)}, settings: settings))
                {
                    daemon.RebuildAll().ConfigureAwait(false).GetAwaiter().GetResult();
                }

                //fails as user is back in the database after rebuilding
                session.Query<User>().Count().Should().Be(0);
            }
        }

        public class User
        {
            public Guid Id { get; set; }
            public string Name { get; set; }

            public void Apply(UserCreated e)
            {
                Id = e.Id;
                Name = e.Name;
            }

            public void Apply(UserDeleted e)
            {
            }
        }

        public class UserCreated
        {
            public Guid Id { get; set; }
            public string Name { get; set; }
        }

        public class UserDeleted
        {
            public Guid Id { get; set; }
        }
    }
}
