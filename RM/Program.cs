using System;
using Balances;

namespace RM
{
    public class Program
    {
        private static readonly log4net.ILog log = log4net.LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod()?.DeclaringType);
        static void Main(string[] args)
        {
            log.Debug($"RM Started at: {DateTime.Now:dd-MM-yyyy H:mm:ss}");
            try
            {
                DateTime date = DateTime.Parse(args[0]);
               // DateTime date = DateTime.Parse("2021/12/21");              
                Console.WriteLine("RM reports running please wait...");
                Balance.ReadFinancialContext(date);
                Balance.ExportResultsTxt("0|SUCCESS");
                log.Info($"Job executed successfully at: {DateTime.Now:dd-MM-yyyy H:mm:ss}");
                Environment.Exit(0);
            }
            catch (Exception ex)
            {
                Balance.ExportResultsTxt("1|FAILURE");
                log.Error("Job Failed:\n " + ex.Message);
                throw ex;
            }
        }

        private static DateTime GetDateValue()
        {
            while (true)
            {
                Console.Write("Please enter a vaild Date (yyyy/mm/dd): ");
                var input = Console.ReadLine();

                if (string.IsNullOrWhiteSpace(input)) continue;

                DateTime.TryParse(input, out var date);

                if (!(date > new DateTime(1900, 1, 3) && date < new DateTime(2100, 12, 31)))
                {
                    Console.WriteLine("Wrong value please try again...");
                    continue;
                }
                Console.Clear();
                return date;
            }
        }
    }
}

