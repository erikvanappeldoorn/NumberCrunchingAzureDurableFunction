using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.DurableTask;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Extensions.Logging;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;

namespace NForza.Demo
{
    public static class NumberCruncherFunction
    {
        [FunctionName("OrchestratorFunction")]
        public static async Task<int> RunOrchestrator(
            [OrchestrationTrigger] IDurableOrchestrationContext context, ILogger log)
        {
            (int max, byte parallel) = context.GetInput<(int,byte)>();

            var cruncher = new NumberCruncher();
            var boundaries = cruncher.GetBoundaries(max, parallel);

            var tasks = new List<Task<int>>();

            foreach (var boundary in boundaries)
            {
                tasks.Add(context.CallActivityAsync<int>("CalculatePrimes", boundary));   
            }

            await Task.WhenAll(tasks);

            int total = tasks.Sum(task => task.Result);

            log.LogInformation($"*** READY: {total} primes found in the range 1 - {max} ***"); 

            return total;
        }

        [FunctionName("CalculatePrimes")]
        public static int CalculatePrimes([ActivityTrigger] (int start, int end) boundary, ILogger log)
        {
            var cruncher = new NumberCruncher();
            int result = 0;
            for (long number = boundary.start; number <= boundary.end; number++)
            {
                if (cruncher.IsPrime(number))
                {
                    result++;
                }
            }

            log.LogInformation($"{result} primes found in the range {boundary.start} - {boundary.end}");
            return result;
        }

        [FunctionName("ClientFunction")]
        public static async Task<HttpResponseMessage> HttpStart(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", "post")] HttpRequestMessage req,
            [DurableClient] IDurableOrchestrationClient starter,
            ILogger log)
        {
            long max = long.Parse(req.RequestUri.ParseQueryString()["max"]);
            byte parallel = byte.Parse(req.RequestUri.ParseQueryString()["parallel"]);
            
            string instanceId = await starter.StartNewAsync("OrchestratorFunction", null, (max, parallel));

            log.LogInformation($"Started orchestration with ID = '{instanceId}'.");

            return starter.CreateCheckStatusResponse(req, instanceId);
        }
    }
}