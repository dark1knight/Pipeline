using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using CsvHelper;
using CsvHelper.Configuration;
using Nest;


namespace Pipeline2
{
    class Program
    {
        static void Main(string[] args)
        {
            string csvFilePath = GetCsvFilePath();
            var queue = ProcessCsvFile(csvFilePath);
            SearchDataByDate(new DateTime(2001, 6, 1), new DateTime(2001, 10, 31));
        }

        static string GetCsvFilePath()
        {
            Console.WriteLine("Enter the path of the CSV file:");
            string filePath = Console.ReadLine();

            // You can add validation or error handling here if needed

            return filePath;
        }

        static Queue<CsvData> ProcessCsvFile(string csvFilePath)
        {
            var queue = new Queue<CsvData>();
            using (var reader = new StreamReader(csvFilePath))
            using (var csv = new CsvReader(reader, System.Globalization.CultureInfo.InvariantCulture))
            {
               
                while (csv.Read())
                {
                    var csvData = csv.GetRecord<CsvData>();
                    queue.Enqueue(csvData);

                    if (queue.Count >= 1000)
                    {
                        ProcessQueue(queue);
                    }
                }
            }

            ProcessQueue(queue);

            return queue;
        }

        static void ProcessQueue(Queue<CsvData> queue)
        {
            var connectionSettings = new ConnectionSettings(new Uri("http://localhost:9200"));
            var client = new ElasticClient(connectionSettings);

            var bulkRequest = new BulkRequest
            {
                Operations = new List<IBulkOperation>()
            };

            while (queue.Count > 0)
            {
                var csvData = queue.Dequeue();
                if (csvData != null)
                {
                    Console.WriteLine($"Processing item: {csvData.publish_time}");

                    var indexOperation = new BulkIndexOperation<CsvData>(csvData);
                    indexOperation.Id = csvData.cord_uid;
                    indexOperation.Index = "covid-19";
                    if (indexOperation != null)
                    {
                        bulkRequest.Operations.Add(indexOperation);
                        Console.WriteLine($"Index operation added: {indexOperation.Id}, {indexOperation.Index}");
                    }
                    else
                    {
                        Console.WriteLine("Failed to create index operation");
                       
                    }
                }
                else
                {
                    Console.WriteLine("Invalid CSV data");
                    
                }

                if (bulkRequest.Operations.Count >= 1000)
                {
                    var bulkResponse = client.Bulk(bulkRequest);
                    if (!bulkResponse.IsValid)
                    {
                        Console.WriteLine("Bulk request failed");
                        
                    }

                    bulkRequest.Operations.Clear();
                }
            }
            if (bulkRequest.Operations.Count > 0)
            {
                var bulkResponse = client.Bulk(bulkRequest);
                if (!bulkResponse.IsValid)
                {
                    Console.WriteLine("Bulk request failed");
                  
                }
            }
        }

        static void SearchDataByDate(DateTime startDate, DateTime endDate)
        {
            var connectionSettings = new ConnectionSettings(new Uri("http://localhost:9200"));
            var client = new ElasticClient(connectionSettings);

            var searchResponse = client.Search<CsvData>(s => s
                .Index("covid-19")
                .Query(q => q
                    .DateRange(dr => dr
                        .Field(f => f.publish_time)
                        .GreaterThanOrEquals(startDate)
                        .LessThanOrEquals(endDate)
                    )
                )
            );

            if (searchResponse.IsValid)
            {
                var hits = searchResponse.Hits;
                Console.WriteLine($"Found {hits.Count} documents:");

                foreach (var hit in hits)
                {
                    var csvData = hit.Source;
                    Console.WriteLine($"- {csvData.publish_time}");
                }
            }
            else
            {
                Console.WriteLine("Search request failed");
            }
        }
    }

    class CsvData
    {
        public string cord_uid { get; set; }
        public string sha { get; set; }
        public string source_x { get; set; }
        public string title { get; set; }
        public string doi { get; set; }
        public string pmcid { get; set; }
        public string pubmed_id { get; set; }
        public string license { get; set; }
        public string publish_time { get; set; }
        public string authors { get; set; }
        public string journal { get; set; }
        public string mag_id { get; set; }
        public string who_covidence_id { get; set; }
        public string arxiv_id { get; set; }
        public string pdf_json_files { get; set; }
        public string pmc_json_files { get; set; }
        public string url { get; set; }
        public string s2_id { get; set; }

    }
}
