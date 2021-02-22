using CloudNative.CloudEvents;
using CloudNative.CloudEvents.Extensions;

using System;
using System.Net.Mime;

namespace EdwardHsu.Lab.CloudEvents
{
    class Program
    {
        static void Main(string[] args)
        {
            var cloudEvent = new CloudEvent(
                   "com.github.pull.create",
                   new Uri("https://github.com/cloudevents/spec/pull/123"))
            {
                DataContentType = new ContentType("application/json"),
                Data = "[]"
            };
        }
    }
}
