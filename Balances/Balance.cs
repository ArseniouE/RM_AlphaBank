using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using Npgsql;
using Model;
using PostgreSQLCopyHelper;


namespace Balances
{
    public class Balance
    {
        
        private static readonly string Path = ConfigurationManager.AppSettings["Path"].ToString();

        private static readonly string Connstr = ConfigurationManager.AppSettings["Connstr"].ToString();

        private static readonly log4net.ILog Log = log4net.LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod()?.DeclaringType);

        private static NpgsqlConnection GetConn()
        {

             //return new NpgsqlConnection(@"Server=10.12.77.103;Port=5432;User Id=olap;Password=0l@p;Database=olap;;Timeout=300;CommandTimeout=300;");
            //  return new NpgsqlConnection(@"Server=10.12.77.105;Port=5432;User Id=x00549;Password=C@$$@ndr@15;Database=olap;Timeout=500;CommandTimeout=500;");
            return new NpgsqlConnection(@"Server=10.12.77.105;Port=5432;User Id=readabrsuat;Password=P@ssw0rd;Database=olap;Timeout=500;CommandTimeout=500;");
            // return new NpgsqlConnection(Connstr);
        }
        public static void ReadFinancialContext(DateTime date)
        {
            Log.Debug($"ReadFinancialContext started at: {DateTime.Now:dd-MM-yyyy H:mm:ss}");
            
            try
            {
                using (var con = GetConn())
                {
                    con.Open();

                    TruncateTable(con);

                    var dt = GatherDataFromRatingScenario(con, date);

                    var insertQuery = new StringBuilder();
                    var dt1 = new DataTable();
                    foreach (DataRow row in dt.Rows)
                    {
                        var financialContext = row[0].ToString();
                        var entityId = row[1].ToString();

                        DateTime appr = Convert.ToDateTime(row[2]);
                        string approvedDate = appr.ToString("yyyy-MM-dd HH:mm:ss.ffffff");

                        DateTime populatedate = Convert.ToDateTime(row[4]);
                        string sourcepopulateddate = populatedate.ToString("yyyy-MM-dd HH:mm:ss.ffffff");

                        ///////////////////////Find the EntityVersion//////////////////
                        //var entityIdWithVersion = FindEntityVersion(financialContext, entityId);
                        var EntityVersion = FindEVersion(financialContext, entityId);
                        ////////////////////////////////////////////////////////////////

                        ////////////////////find financialid////////////////////
                        int FinancialId = FindFinancialId(financialContext);
                      
                        var result = financialContext.Substring(financialContext.LastIndexOf('#') + 1);

                        if (result.Length == 0) continue;

                        var array = result.Split(';').ToList();

                        var orderedstatementids = GetFinancialIds(FinancialId, financialContext, con);

                        int i = 0;
                        var StatementIds = new List<int>();

                        StatementIds.AddRange(orderedstatementids);
                       // StatementIds = orderedstatementids;

                        foreach (var roww in orderedstatementids)
                        {

                            var firstNumber = roww.ToString();
                            ///////////////////////////////////find all the statement ids that participate in the scenario////////////////////
                            

                            ///////////////find changeincommonsharecapital and sharepremium ////////////////////
                            decimal? FlowsCommonShareCapital= 0.0011M; 

                            if (i == 0)
                            {
                                FlowsCommonShareCapital = 0.0011M;
                            }
                            else
                            {
                                if (StatementIds.Count > 1)
                                {
                                    var PrevYearvalues = GetPreviousYearChanges(firstNumber,date, entityId,financialContext, con,sourcepopulateddate,approvedDate, FinancialId, StatementIds);
                                    
                                    if (PrevYearvalues == null)
                                    {
                                        FlowsCommonShareCapital = 0.0011M;
                                    }
                                    else
                                    {

                                        FlowsCommonShareCapital = Convert.ToDecimal(PrevYearvalues);
                                    }
                                }
                                
                            }
                                                       
                            var cmd1 = GetFinancialFromHistorical(firstNumber, EntityVersion, con, entityId, date, financialContext, FinancialId, sourcepopulateddate, approvedDate, FlowsCommonShareCapital);
                            var data = new NpgsqlDataAdapter(cmd1);
                            i++;
                            try
                            {
                                data.Fill(dt1);

                            }
                            catch (Exception e)
                            {
                                Log.Error("Get the financial from historical statement failed:\n" + e.Message + "\n" + e.StackTrace + "\n" + entityId + "\n" + firstNumber);
                            }
                        }
                    }


                    //  ExportToCsv(dt1, Path, date);
                    
                    Log.Debug($"Fullfill The list started at: {DateTime.Now:dd-MM-yyyy H:mm:ss}");

                    var MyList = FillTheList(dt1);

                    foreach(var row in MyList)
                    {
                        if (row.ChgCommonShareCapital_ChgSharePremium == 0.0011M)
                        {
                            row.ChgCommonShareCapital_ChgSharePremium = null;
                        }
                        
                    }

                     SendToPostGre(MyList, con);
                
                     FindInventory(con);

                     FindNettradereceivables(con);

                     FindTradespayable(con);

                     FindTotalBankingDebts( con);

                     FindShortTermBankingDebt( con);

                     FindLongTermBankingDebt(con);

                     Finddividendspayables(con);

                     Findinterestcoverage(con);

                    Updatetotalbankingdepttoebitda(con);

                    Updatenetbankingdepttoebitda(con);

                    var DtTable = ReadFromDatabase(con);

                      ExportToCsv(DtTable, Path, date);
                    //var Table= CreateTableForDat(dt1, date);

                    //ExportToDat(Table, Path, date);


                }

            }
            catch (Exception e)
            {
                Log.Error("Get the financial from historical statement failed:\n" + e.Message + "\n" + e.StackTrace + "\n");
            }
        }

        
        private static DataTable ReadFromDatabase(NpgsqlConnection con)
        {
            //

            var dt = new DataTable();

            string query = $@"select  cdi,afm,csh,ebitda,eqty,gdwill,nt_incm,sales_revenue,netfixedassets,inventory,nettradereceivables,totalassets,commonsharecapital,
                              tradespayable,totalbankingdebt,shorttermbankingdebt,longtermbankingdebt,totalliabilities,grossprofit,ebit,profitbeforetax,workingcapital,
                              flowsoperationalactivity,flowsinvestmentactivity,flowsfinancingactivity,chgcommonsharecapital_chgsharepremium,balancedividendspayable,
                              grossprofitmargin,netprofitmargin,ebitdamargin,totalbankingdebttoebitda,netbankingdebttoebitda ,totalliabilitiestototalequity ,
                              returnonassets,returnonequity,
                              case when interestcoverage  = 0.00 then 0.00 else (ebitda / interestcoverage)::numeric(19,2) end as interestcoverage ,
                              currentratio,quickratio,fnc_year ,publish_date,approveddate,reference_date,entityid 
	                          from olapts.rmtemptable";

           

            var cmd = new NpgsqlCommand(query, con);
            var da = new NpgsqlDataAdapter(cmd);

            try
            {
                da.Fill(dt);
                Log.Debug($"Read final table from database executed successfully at: {DateTime.Now:dd-MM-yyyy H:mm:ss}");
            }
            catch (Exception e)
            {
                Log.Error("GatherDataFromRatingScenario failed : \n" + e.Message + e.StackTrace);
            }

            return dt;          
        }

        private static void Updatenetbankingdepttoebitda(NpgsqlConnection con)
        {
            string query = $@"update olapts.rmtemptable
                                    set netbankingdebttoebitda = 
                                    case when ebitda = 0.00  
									     then 0.00
									     else  ((totalbankingdebt - csh)  /  ebitda)
								end ";

            NpgsqlCommand command = new NpgsqlCommand(query, con);
            var table = command.ExecuteNonQuery();
        }
        
        private static void Updatetotalbankingdepttoebitda(NpgsqlConnection con)
        {
            string query = $@"update olapts.rmtemptable set totalbankingdebttoebitda = case when ebitda = 0.00  
							                                            then 0.00
							                         else (totalbankingdebt /  ebitda)
						                                                        end ";

            NpgsqlCommand command = new NpgsqlCommand(query, con);
            var table = command.ExecuteNonQuery();
        }

        private static void TruncateTable(NpgsqlConnection con)
        {
            string query = $@"Truncate table olapts.rmtemptable ";

            NpgsqlCommand command = new NpgsqlCommand(query, con);
            var table = command.ExecuteNonQuery();
        }

        private static void Findinterestcoverage( NpgsqlConnection con)
        {
            string query = $@"update olapts.rmtemptable
                              set interestcoverage= olapts.Returninterestcoverage(olapts.rmtemptable.financialid::text,olapts.rmtemptable.statementid)";

                NpgsqlCommand command = new NpgsqlCommand(query, con);
                var interestcoverage = command.ExecuteNonQuery();
           
            //string query = $@"SELECT  sum( coalesce((case when originrounding = 0 then originbalance
            //   when originrounding = 1 then originbalance * 1000
            //when originrounding = 2 then originbalance * 100000 end),0))  as interestcoverage
            //             FROM OLAPTS.facthiststmtbalancelatest  
            //             where financialid::int = {Financialid} and statementid = {Statementid}
            //                   and accountid = 3400";

            //using (NpgsqlConnection connection = new NpgsqlConnection(Connstr))
            //{
            //    NpgsqlCommand command = new NpgsqlCommand(query, con);
            //    var interestcoverage = command.ExecuteScalar();

            //    if (interestcoverage != DBNull.Value)
            //    {
            //        return Convert.ToInt32(interestcoverage);
            //    }
            //    else
            //    {
            //        return 0;
            //    }
            //}
        }

        private static void Finddividendspayables( NpgsqlConnection con)
        {

            string query = $@"update olapts.rmtemptable
                              set balancedividendspayable= olapts.Returndividendspayables(olapts.rmtemptable.financialid::text,olapts.rmtemptable.statementid)";

           
                NpgsqlCommand command = new NpgsqlCommand(query, con);
                var dividendspayable = command.ExecuteNonQuery();

            //string query = $@"SELECT  sum( coalesce((case when originrounding = 0 then originbalance
            //   when originrounding = 1 then originbalance * 1000
            //when originrounding = 2 then originbalance * 100000 end),0))  as dividendspayables
            //             FROM OLAPTS.facthiststmtbalancelatest  
            //             where financialid::int = {Financialid} and statementid = {Statementid}
            //                   and accountid in (5950,5960)";


            //using (NpgsqlConnection connection = new NpgsqlConnection(Connstr))
            //{
            //    NpgsqlCommand command = new NpgsqlCommand(query, con);
            //     var dividendspayables = command.ExecuteScalar();

            //    if (dividendspayables != DBNull.Value)
            //    {
            //        return Convert.ToInt32(dividendspayables);
            //    }
            //    else
            //    {
            //        return 0;
            //    }               
        }
 
        private static void FindLongTermBankingDebt( NpgsqlConnection con)
        {
            string query = $@"update olapts.rmtemptable
                                set longtermbankingdebt = olapts.ReturnLongtermBankingDept(olapts.rmtemptable.financialid::text,olapts.rmtemptable.statementid)";

                NpgsqlCommand command = new NpgsqlCommand(query, con);
                var LongTermBankingDebt = command.ExecuteNonQuery();

            //string query = $@"SELECT  sum( coalesce((case when originrounding = 0 then originbalance
            //   when originrounding = 1 then originbalance * 1000
            //when originrounding = 2 then originbalance * 100000 end),0))  as LongTermBankingDebt
            //             FROM OLAPTS.facthiststmtbalancelatest  
            //             where financialid::int = {Financialid} and statementid = {Statementid}
            //                   and accountid in (2100,2110,2115,2120,2130,2150)";

            //using (NpgsqlConnection connection = new NpgsqlConnection(Connstr))
            //{
            //    NpgsqlCommand command = new NpgsqlCommand(query, con);
            //    var LongTermBankingDebt = command.ExecuteScalar();

            //    if (LongTermBankingDebt != DBNull.Value)
            //    {
            //        return Convert.ToInt32(LongTermBankingDebt);
            //    }
            //    else
            //    {
            //        return 0;
            //    }
            //}
        }

        private static void FindShortTermBankingDebt( NpgsqlConnection con)
        {
            string query = $@"update olapts.rmtemptable
                               set shorttermbankingdebt = olapts.ReturnShorttermBankingDept(olapts.rmtemptable.financialid::text,olapts.rmtemptable.statementid)";

            
                NpgsqlCommand command = new NpgsqlCommand(query, con);
                var ShortTermBankingDebt = command.ExecuteNonQuery();
           
            //string query = $@"SELECT  sum( coalesce((case when originrounding = 0 then originbalance
            //   when originrounding = 1 then originbalance * 1000
            //when originrounding = 2 then originbalance * 100000 end),0))  as ShortTermBankingDebt
            //             FROM OLAPTS.facthiststmtbalancelatest  
            //             where financialid::int = {Financialid} and statementid = {Statementid}
            //                   and accountid in (2400,2410,2415,2420,2430,2440,2450,2460,2470)";

            //using (NpgsqlConnection connection = new NpgsqlConnection(Connstr))
            //{
            //    NpgsqlCommand command = new NpgsqlCommand(query, con);
            //    var ShortTermBankingDebt = command.ExecuteScalar();


            //    if (ShortTermBankingDebt != DBNull.Value)
            //    {
            //        return Convert.ToInt32(ShortTermBankingDebt);
            //    }
            //    else
            //    {
            //        return 0;
            //    }             
            //}
        }
        
        private static void FindTotalBankingDebts( NpgsqlConnection con)
        {

            string query = $@"update olapts.rmtemptable
                              set totalbankingdebt = olapts.ReturnTotalBankingDept(olapts.rmtemptable.financialid::text,olapts.rmtemptable.statementid)";
            
                NpgsqlCommand command = new NpgsqlCommand(query, con);
                var totalBankingDebt = command.ExecuteNonQuery();
    
            //string query = $@"SELECT  sum( coalesce((case when originrounding = 0 then originbalance
            //   when originrounding = 1 then originbalance * 1000
            //when originrounding = 2 then originbalance * 100000 end),0))  as TotalBankingDebt
            //             FROM OLAPTS.facthiststmtbalancelatest  
            //             where financialid::int = {Financialid} and statementid = {Statementid}
            //                   and accountid in (2100,2110,2115,2120,2130,2150,2400,2410,2415,2420,2430,2440,2450,2460,2470)";

            //using (NpgsqlConnection connection = new NpgsqlConnection(Connstr))
            //{
            //    NpgsqlCommand command = new NpgsqlCommand(query, con);
            //    var TotalBankingDebt = command.ExecuteScalar();

            //    if (TotalBankingDebt != DBNull.Value)
            //    {
            //        return Convert.ToInt32(TotalBankingDebt);
            //    }
            //    else
            //    {
            //        return 0;
            //    }

            //}  
        }

        private static void FindInventory( NpgsqlConnection con)
        {
            string query = $@"update olapts.rmtemptable
                               set inventory = olapts.ReturnInventories(olapts.rmtemptable.financialid::text,olapts.rmtemptable.statementid)";
              
                NpgsqlCommand command = new NpgsqlCommand(query, con);
                var inventory = command.ExecuteNonQuery();
        }

        private static void FindNettradereceivables( NpgsqlConnection con)
        {

            string query = $@"update olapts.rmtemptable
                              set Nettradereceivables= olapts.ReturnNettradereceivables(olapts.rmtemptable.financialid::text,olapts.rmtemptable.statementid)";
            
                NpgsqlCommand command = new NpgsqlCommand(query, con);
                var Nettradereceivables = command.ExecuteNonQuery();
            
        }

        private static void FindTradespayable( NpgsqlConnection con)
        {

            string query = $@"update olapts.rmtemptable
                                set tradespayable = olapts.ReturnTradesPayable(olapts.rmtemptable.financialid::text,olapts.rmtemptable.statementid)";

                NpgsqlCommand command = new NpgsqlCommand(query, con);
                var tradespayable = command.ExecuteNonQuery();

            //string query = $@"SELECT  sum( coalesce((case when originrounding = 0 then originbalance
            //   when originrounding = 1 then originbalance * 1000
            //when originrounding = 2 then originbalance * 100000 end),0))  as inventories
            //             FROM OLAPTS.facthiststmtbalancelatest  
            //             where financialid::int = {Financialid} and statementid = {Statementid}
            //                   and accountid in (2680,2685,2686,2687)";

            //using (NpgsqlConnection connection = new NpgsqlConnection(Connstr))
            //{
            //    NpgsqlCommand command = new NpgsqlCommand(query, con);
            //    var Tradespayable = command.ExecuteScalar();

            //    if (Tradespayable != DBNull.Value)
            //    {
            //        return Convert.ToInt32(Tradespayable);
            //    }
            //    else
            //    {
            //        return 0;
            //    }  
            //}
        }


        private static void SendToPostGre(List<Rmfields> MyList, NpgsqlConnection con)
        {
           try
            {
                var copyHelper = new PostgreSQLCopyHelper<Rmfields>("olapts", "RmTempTable")
                .MapCharacter("cdi", x => x.cdi)
                .MapCharacter("afm", x => x.afm)
                .MapNumeric("csh", x => x.csh)
                .MapNumeric("ebitda", x => x.ebitda)
                .MapNumeric("eqty", x => x.eqty)
                .MapNumeric("gdwill", x => x.gdwill)
                .MapNumeric("nt_incm", x => x.nt_incm)
                .MapNumeric("sales_revenue", x => x.sales_revenue)
                .MapNumeric("netfixedAssets", x => x.netfixedassets)
                .MapNumeric("inventory", x => x.inventory)
                .MapNumeric("nettradereceivables", x => x.nettradereceivables)
                .MapNumeric("totalassets", x => x.TotalAssets)
                .MapNumeric("commonsharecapital", x => x.CommonShareCapital)
                .MapNumeric("tradespayable", x => x.TradesPayable)
                .MapNumeric("totalbankingdebt", x => x.TotalBankingDebt)
                .MapNumeric("shorttermbankingdebt", x => x.ShortTermBankingDebt)
                .MapNumeric("longtermbankingdebt", x => x.LongTermBankingDebt)
                .MapNumeric("totalliabilities", x => x.TotalLiabilities)
                .MapNumeric("grossprofit", x => x.GrossProfit)
                .MapNumeric("ebit", x => x.Ebit)
                .MapNumeric("profitbeforetax", x => x.ProfitBeforeTax)
                .MapNumeric("workingcapital", x => x.WorkingCapital)
                .MapNumeric("flowsoperationalactivity", x => x.FlowsOperationalActivity)
                .MapNumeric("flowsinvestmentactivity", x => x.FlowsInvestmentActivity)
                .MapNumeric("flowsfinancingactivity", x => x.FlowsFinancingActivity)
                .MapNumeric("chgcommonsharecapital_chgsharepremium", x => x.ChgCommonShareCapital_ChgSharePremium)
                .MapNumeric("balancedividendspayable", x => x.Balancedividendspayable)
                .MapNumeric("grossprofitmargin", x => x.GrossProfitMargin)
                .MapNumeric("netprofitmargin", x => x.NetProfitMargin)
                .MapNumeric("ebitdamargin", x => x.EbitdaMargin)
                .MapNumeric("totalbankingdebttoebitda", x => x.TotalBankingDebttoEbitda)
                .MapNumeric("netbankingdebttoebitda", x => x.NetBankingDebttoEbitda)
                .MapNumeric("totalliabilitiestototalequity", x => x.TotalLiabilitiestoTotalEquity)
                .MapNumeric("returnonassets", x => x.ReturnOnAssets)
                .MapNumeric("returnonequity", x => x.ReturnonEquity)
                .MapNumeric("interestcoverage", x => x.interestcoverage)
                .MapNumeric("currentratio", x => x.CurrentRatio)
                .MapNumeric("quickratio", x => x.QuickRatio)
                .MapCharacter("fnc_year", x => x.fnc_year)
                .MapCharacter("publish_date", x => x.publish_date)
                .MapCharacter("approveddate", x => x.approveddate)
                .MapCharacter("reference_date", x => x.reference_date)
                .MapCharacter("entityid", x => x.entityid)
                .MapInteger("FinancialId", x => x.FinancialId)
                .MapInteger("Statementid", x => x.Statementid);


                Log.Debug($"Save To Postgre  started at: {DateTime.Now:dd-MM-yyyy H:mm:ss}");
                copyHelper.SaveAll(con, MyList);
            }
            catch(Exception ex )
            {
                    Log.Error("save to Postgre Failed at:\n" + ex.Message + "\n" + ex.StackTrace + "\n");                   
            }
            
            
          
        }

        private static List<Rmfields> FillTheList(DataTable dt1)
        {
            List<Rmfields> items = new List<Rmfields>();
            try
            {

                foreach (DataRow row in dt1.Rows)
                {
                    var rmrow = new Rmfields
                    {
                        cdi = row["cdi"].ToString(),
                        afm = row["afm"].ToString(),
                        csh =  Convert.ToDecimal(row["csh"].ToString()),
                        ebitda = Convert.ToDecimal(row["ebitda"].ToString()),
                        eqty = Convert.ToDecimal(row["eqty"].ToString()),
                        gdwill = Convert.ToDecimal(row["gdwill"].ToString()),
                        nt_incm = Convert.ToDecimal(row["nt_incm"].ToString()),
                        sales_revenue = Convert.ToDecimal(row["sales_revenue"].ToString()),
                        netfixedassets = Convert.ToDecimal(row["netfixedassets"].ToString()),
                        inventory = Convert.ToDecimal(row["inventory"].ToString()),
                        nettradereceivables = Convert.ToDecimal(row["nettradereceivables"].ToString()),
                        TotalAssets = Convert.ToDecimal(row["TotalAssets"].ToString()),
                        CommonShareCapital = Convert.ToDecimal(row["CommonShareCapital"].ToString()),
                        TradesPayable = Convert.ToDecimal(row["TradesPayable"].ToString()),
                        TotalBankingDebt = Convert.ToDecimal(row["TotalBankingDebt"].ToString()),
                        ShortTermBankingDebt = Convert.ToDecimal(row["ShortTermBankingDebt"].ToString()),
                        LongTermBankingDebt = Convert.ToDecimal(row["LongTermBankingDebt"].ToString()),
                        TotalLiabilities= Convert.ToDecimal(row["TotalLiabilities"].ToString()),
                        GrossProfit= Convert.ToDecimal(row["GrossProfit"].ToString()),
                        Ebit = Convert.ToDecimal(row["Ebit"].ToString()),
                        ProfitBeforeTax = Convert.ToDecimal(row["ProfitBeforeTax"].ToString()),
                        WorkingCapital = Convert.ToDecimal(row["WorkingCapital"].ToString()),
                        FlowsOperationalActivity = Convert.ToDecimal(row["FlowsOperationalActivity"].ToString()),
                        FlowsInvestmentActivity = Convert.ToDecimal(row["FlowsInvestmentActivity"].ToString()),
                        FlowsFinancingActivity = Convert.ToDecimal(row["FlowsFinancingActivity"].ToString()),
                        ChgCommonShareCapital_ChgSharePremium = string.IsNullOrWhiteSpace(row["ChgCommonShareCapital_ChgSharePremium"].ToString()) ? 0.0011M : Convert.ToDecimal(row["ChgCommonShareCapital_ChgSharePremium"].ToString()),
                        Balancedividendspayable = Convert.ToDecimal(row["Balancedividendspayable"].ToString()),
                        GrossProfitMargin = Convert.ToDecimal(row["GrossProfitMargin"].ToString()),
                        NetProfitMargin = Convert.ToDecimal(row["NetProfitMargin"].ToString()),
                        EbitdaMargin = Convert.ToDecimal(row["EbitdaMargin"].ToString()),
                        TotalBankingDebttoEbitda = Convert.ToDecimal(row["TotalBankingDebttoEbitda"].ToString()),
                        NetBankingDebttoEbitda = Convert.ToDecimal(row["NetBankingDebttoEbitda"].ToString()),
                        TotalLiabilitiestoTotalEquity = Convert.ToDecimal(row["TotalLiabilitiestoTotalEquity"].ToString()),
                        ReturnOnAssets = Convert.ToDecimal(row["ReturnOnAssets"].ToString()),
                        ReturnonEquity = Convert.ToDecimal(row["ReturnonEquity"].ToString()),
                        interestcoverage = Convert.ToDecimal(row["interestcoverage"].ToString()),
                        CurrentRatio = Convert.ToDecimal(row["currentratio"].ToString()),
                        QuickRatio= Convert.ToDecimal(row["quickratio"].ToString()),
                        fnc_year = row["fnc_year"].ToString(),
                        publish_date = row["publish_date"].ToString(),
                        approveddate= row["approveddate"].ToString(),
                        reference_date = row["reference_date"].ToString(),
                        entityid = row["entityid"].ToString(),
                        FinancialId = Convert.ToInt32(row["FinancialId"].ToString()),
                        Statementid = Convert.ToInt32(row["Statementid"].ToString())
                        
                    };
                    items.Add(rmrow);
                 }
            
                //List<Rmfields> items = dt1.AsEnumerable().Select(row =>
                //        new Rmfields
                //        {
                //            cdi = row.Field<string>("cdi"),
                //            afm = row.Field<string>("afm"),
                //            csh = row.Field<decimal>("csh"),
                //            ebitda = row.Field<decimal>("ebitda"),
                //            eqty = row.Field<decimal>("eqty"),
                //            gdwill = row.Field<decimal>("gdwill"),
                //            nt_incm = row.Field<decimal>("nt_incm"),
                //            sales_revenue = row.Field<decimal>("sales_revenue"),
                //            netfixedassets = row.Field<decimal>("netfixedassets"),
                //            inventory = row.Field<decimal>("inventory"),
                //            nettradereceivables = row.Field<decimal>("nettradereceivables"),
                //            TotalAssets = row.Field<decimal>("TotalAssets"),
                //            CommonShareCapital = row.Field<decimal>("CommonShareCapital"),
                //            TradesPayable = row.Field<decimal>("TradesPayable"),
                //            TotalBankingDebt = row.Field<decimal>("TotalBankingDebt"),
                //            ShortTermBankingDebt = row.Field<decimal>("ShortTermBankingDebt"),
                //            LongTermBankingDebt = row.Field<decimal>("LongTermBankingDebt"),
                //            TotalLiabilities = row.Field<decimal>("TotalLiabilities"),
                //            GrossProfit = row.Field<decimal>("GrossProfit"),
                //            Ebit = row.Field<decimal>("Ebit"),
                //            ProfitBeforeTax = row.Field<decimal>("ProfitBeforeTax"),
                //            WorkingCapital = row.Field<decimal>("WorkingCapital"),
                //            FlowsOperationalActivity = row.Field<decimal>("FlowsOperationalActivity"),
                //            FlowsInvestmentActivity = row.Field<decimal>("FlowsInvestmentActivity"),
                //            FlowsFinancingActivity = row.Field<decimal>("FlowsFinancingActivity"),
                //            ChgCommonShareCapital_ChgSharePremium = row.Field<decimal?>("ChgCommonShareCapital_ChgSharePremium")?null: row.Field<decimal?>("ChgCommonShareCapital_ChgSharePremium"),
                //            ChgCommonShareCapital_ChgSharePremium = row.Field<decimal>("ChgCommonShareCapital_ChgSharePremium"),
                //            Balancedividendspayable = row.Field<decimal>("Balancedividendspayable"),
                //            GrossProfitMargin = row.Field<decimal>("GrossProfitMargin"),
                //            NetProfitMargin = row.Field<decimal>("NetProfitMargin"),
                //            EbitdaMargin = row.Field<decimal>("EbitdaMargin"),
                //            TotalBankingDebttoEbitda = row.Field<decimal>("TotalBankingDebttoEbitda"),
                //            NetBankingDebttoEbitda = row.Field<decimal>("NetBankingDebttoEbitda"),
                //            TotalLiabilitiestoTotalEquity = row.Field<decimal>("TotalLiabilitiestoTotalEquity"),
                //            ReturnOnAssets = row.Field<decimal>("ReturnOnAssets"),
                //            ReturnonEquity = row.Field<decimal>("ReturnonEquity"),
                //            interestcoverage = row.Field<decimal>("interestcoverage"),
                //            CurrentRatio = row.Field<decimal>("CurrentRatio"),
                //            fnc_year = row.Field<string>("fnc_year"),
                //            publish_date = row.Field<string>("publish_date"),
                //            approveddate = row.Field<string>("approveddate"),
                //            reference_date = row.Field<string>("reference_date"),
                //            entityid = row.Field<string>("entityid"),
                //            FinancialId = row.Field<int>("FinancialId"),
                //            Statementid = row.Field<int>("Statementid")

                //        }).ToList();

                return items;
            }
            catch(Exception ex)
            {
                Log.Error("Fill The List Failed at:\n" + ex.Message + "\n" + ex.StackTrace + "\n" );
                return null;
            }
            
        }

         
        private static decimal? GetPreviousYearChanges (string firstNumber, DateTime date, string entityid, string FinancialContext, NpgsqlConnection con, string sourcepopulateddate, string approveddate, int FinancialId,List<int> StatementIds)
        {
            try
            {                            
                string query;
                var FirstStatement = StatementIds[0];
                var SecondStatement = StatementIds[1];

                StatementIds.Remove(FirstStatement);

                query = $@"select unionquery.commonsharecapital - lag(unionquery.commonsharecapital) over(order by unionquery.statementdatekey_) +
                        unionquery.sharepremium - lag(unionquery.sharepremium) over(order by unionquery.statementdatekey_) as chg
                        from(
                        select * from(
                        select distinct on(a.pkid_) b.commonsharecapital, b.sharepremium, a.statementdatekey_
                        from olapts.factuphiststmtfinancial a
                        join olapts.factuphiststmtfinancialgift b on a.pkid_ = b.pkid_ and a.versionid_ = b.versionid_
                        join olapts.factratingscenario c on c.entityid = cast(a.entityid as int)
                        where a.entityid = '{entityid}' and a.financialid = '{FinancialId}' and a.statementid = '{Convert.ToInt32(FirstStatement)}'                       
                        and a.sourcepopulateddate_ <= '{sourcepopulateddate}'
                        and cast(c.ApprovedDate as date) >= '2021-01-06' and cast(c.ApprovedDate as date) <= '{date.ToString("yyyy-MM-dd")}'
                        and c.financialcontext = '{FinancialContext}'
                         and c.ApprovedDate = '{approveddate}'
                        and c.modelid = ('FA_FIN')
                        order by a.pkid_, a.sourcepopulateddate_ desc)x
                        union
                        select * from(
                        select distinct on(a.pkid_)
                        b.commonsharecapital, b.sharepremium, a.statementdatekey_
                        from olapts.factuphiststmtfinancial a
                        join olapts.factuphiststmtfinancialgift b on a.pkid_ = b.pkid_ and a.versionid_ = b.versionid_
                        join olapts.factratingscenario c on c.entityid = cast(a.entityid as int)
                        where a.entityid = '{entityid}' and a.financialid = '{FinancialId}' and a.statementid = '{Convert.ToInt32(SecondStatement)}'                     
                        and a.sourcepopulateddate_ <= '{sourcepopulateddate}'
                        and cast(c.ApprovedDate as date) >= '2021-01-06' and cast(c.ApprovedDate as date) <= '{date.ToString("yyyy-MM-dd")}'
                        and c.financialcontext = '{FinancialContext}'
                         and c.ApprovedDate = '{approveddate}'
                        and c.modelid = ('FA_FIN')
                        order by a.pkid_, a.sourcepopulateddate_ desc)second) unionquery
                        order by unionquery.statementdatekey_ desc limit 1";


                //query = $@"  select sumvalues-lag(sumvalues) over(order by aa.ordercolumn) as chg from(
                //             select sum(coalesce((case when originrounding = 0 then originbalance
                //                                       when originrounding = 1 then originbalance * 1000
                //                                       when originrounding = 2 then originbalance * 100000 end), 0)) as sumvalues, 1 as ordercolumn
                //             FROM OLAPTS.facthiststmtbalancelatest a
                //             where a.financialid = '{FinancialId}' and a.statementid = {FirstStatement.ToString()}
                //             and accountid in  (1800, 1820)
                //             union
                //             select sum(coalesce((case when originrounding = 0 then originbalance
                //                                       when originrounding = 1 then originbalance * 1000
                //                                       when originrounding = 2 then originbalance * 100000 end), 0)) , 2 as ordercolumn
                //             FROM OLAPTS.facthiststmtbalancelatest a
                //             where a.financialid = '{FinancialId}' and a.statementid = {SecondStatement.ToString()}
                //             and accountid in  (1800, 1820))aa order by aa.ordercolumn desc   limit 1";



                decimal? value = null;
                
                using (NpgsqlCommand command = new NpgsqlCommand(query, con)) 
                {
                   var newvalue = command.ExecuteScalar();

                    if (string.IsNullOrWhiteSpace(newvalue.ToString()))
                    {
                        value = null; 
                    }
                    else
                    {

                        value = Convert.ToDecimal(newvalue,new CultureInfo("en-US"));
                    }
                    
                    command.Dispose();
                   
                }
                    
                return value;
                
            }
            catch (Exception e)
            {
                Log.Error("GetPreviousYearChanges : \n" + e.Message + e.StackTrace + "\n" + entityid + " " + firstNumber);
                return null;
            }          
        }

        private static List<int>  GetFinancialIds(int financialid,string FinancialContext, NpgsqlConnection con)
        {
            try
            {
                
                List<int> mylist = new List<int>();
                var stringBuilder = new StringBuilder();
                string financial = FinancialContext.Split('#').Last();
                var array = financial.Split(';').ToList();
                
                foreach (var row in array)
                {
                    var n1 = row.IndexOf(':');

                    if (n1 == -1) continue;

                    var firstNumber = row.Substring(0, n1);

                    //mylist.Add(Convert.ToInt16(firstNumber));
                    

                    stringBuilder.Append(firstNumber + ",");
                }

                string AllFinancialIds = stringBuilder.ToString().TrimEnd(',');

                //string query = $@"select distinct qq.statementid from(
                //                  select distinct statementid ,statementdatekey_
                //                  from olapts.factuphiststmtfinancial
                //                  where financialid = {financialid}                
                //                  and statementid in ({AllFinancialIds})
                //                  order by statementdatekey_ asc)qq ";


                string query = $@"select qq.statementid from(
                                  select statementid, max(statementdatekey_)
                                  from olapts.factuphiststmtfinancial
                                  where financialid = {financialid}                
                                  and statementid in ({AllFinancialIds}) 
                                 group by financialid,statementid
                                  order by max(statementdatekey_) asc)qq ";



                var cmd = new NpgsqlCommand(query, con);
                var da = new NpgsqlDataAdapter(cmd);
                var dt = new DataTable();


                try
                {
                    da.Fill(dt);
                    //Log.Debug($"Order The statement List executed successfully at: {DateTime.Now:dd-MM-yyyy H:mm:ss}");
                }
                catch (Exception e)
                {
                    Log.Error("Order The statement List failed : \n" + e.Message + e.StackTrace);
                }

               foreach(DataRow row in dt.Rows)
                {
                    mylist.Add(Convert.ToInt32(row[0]));
                }
               
                return mylist;
            }
            catch(Exception ex)
            {
                Log.Error("GetFinancialIds failed : \n" + ex.Message + ex.StackTrace);
                return null;
            }
            
        }
        private static  DataTable CreateTableForDat(DataTable dt1, DateTime date)
        {
            var data = new DataTable();
            data.Columns.Add("1");
            data.Columns.Add("2");
            data.Columns.Add("3");
            data.Columns.Add("4");
            data.Columns.Add("5");
            data.Columns.Add("6");
            data.Columns.Add("7");
            data.Columns.Add("8");
            data.Columns.Add("9");
            data.Columns.Add("10");
            data.Columns.Add("11");


            foreach (DataRow dt in dt1.Rows)
            {
                DataRow row = data.NewRow();
                row[0] = '2';
                row[1] = dt[0];
                row[2] = dt[1];
                row[3] = "SDP" + date.ToString("yyyyMMdd"); 
                row[4] = long.Parse(dt[7].ToString());
                row[5] = Convert.ToInt64(dt[2]);
                row[6] = Convert.ToInt64(dt[3]);
                row[7] = Convert.ToInt64(dt[4]);
                row[8] = Convert.ToInt64(dt[5]);
                row[9] = Convert.ToInt64(dt[6]);
                row[10] = dt[8];

                data.Rows.Add(row);
            }
  
            return data;
        }

        private static void ExportToDat(DataTable data, string filePath, DateTime date)
        {
            var path = $@"{filePath}\RM_REPORT_{date.ToString("yyyyMMdd").Replace("/", "")}.dat";

            if (File.Exists(path))
            {
                File.Delete(path);
            }

            using (StreamWriter sw = (File.Exists(path) ? File.AppendText(path) : File.CreateText(path)))
            {

                foreach (DataRow dt in data.Rows)
                {

                    var stringBuilder = new StringBuilder();

                    stringBuilder.Append($"{dt[0]} {new string(' ', 9 - dt[1].ToString().Length)}");
                    stringBuilder.Append($"{dt[1]} {new string(' ', 10)}");

                    AddPrependingZeros(stringBuilder, dt, 2, 9);

                    stringBuilder.Append(dt[2].ToString() + new string(' ', 2));
                    stringBuilder.Append(dt[3].ToString() + ' ');
                    stringBuilder.Append(dt[4].ToString());

                    CheckSign(stringBuilder, dt, 5);
                    AddPrependingZeros(stringBuilder, dt, 5, 16);
                    stringBuilder.Append(dt[5].ToString() + ",00");

                    CheckSign(stringBuilder, dt, 6);
                    AddPrependingZeros(stringBuilder, dt, 6, 16);
                    stringBuilder.Append(dt[6].ToString() + ",00");

                    CheckSign(stringBuilder, dt, 7);
                    AddPrependingZeros(stringBuilder, dt, 7, 16);
                    stringBuilder.Append(dt[7].ToString() + ",00");

                    CheckSign(stringBuilder, dt, 8);
                    AddPrependingZeros(stringBuilder, dt, 8, 16);
                    stringBuilder.Append(dt[8].ToString() + ",00");


                    CheckSign(stringBuilder, dt, 9);
                    AddPrependingZeros(stringBuilder, dt, 9, 16);
                    stringBuilder.Append(dt[9].ToString() + ",");

                    AddPrependingZeros(stringBuilder, dt, 10, 10);
                    stringBuilder.Append(dt[10].ToString());

                    sw.Write(stringBuilder.ToString() + "\n");

                }
                 
            }
            
        }

        private static void CheckSign(StringBuilder stringBuilder, DataRow dt, int index)
        {
            stringBuilder.Append(long.Parse(dt[index].ToString()) >= 0 ? "+" : "-");
            RemoveNegativeSign(dt, index);
        }
        
        private static void RemoveNegativeSign(DataRow dt, int index)
        {
            dt[index] = dt[index].ToString().Replace("-", "");
        }
        
        private static void AddPrependingZeros(StringBuilder stringBuilder, DataRow dt, int index, int length)
        {
            if (dt[index].ToString().Length < length)
            {
                var zerosStr = "";
                var zeros = length - dt[index].ToString().Length;

                for (var i = 0; i < zeros; i++)
                {
                    zerosStr += "0";
                }

                stringBuilder.Append(zerosStr);
            }
        }

        private static DataTable GatherDataFromRatingScenario(NpgsqlConnection con, DateTime date)
        {
            var dt = new DataTable();
            string query =
                   $@"select distinct on (EntityId) 
                        FinancialContext  as FinancialContext,
                        EntityId  as Entityid ,
                        ApprovedDate  as ApprovedDate,
                        Updateddate_ as UpdatedDat,
                        sourcepopulateddate_ as sourcepopulateddat
                        from olapts.factRatingScenario  
                        where cast( ApprovedDate as date) >= '2021-01-06' and cast(ApprovedDate as date) <= '{date.ToString("yyyy-MM-dd")}'  
                        and isdeleted_ = 'false' and IsLatestApprovedScenario = 'true'  
                        and IsPrimary = 'true' and FinancialContext is not null and FinancialContext <> '###' and modelid = ('FA_FIN') 
                        and FinancialContext <> '0' and FinancialContext <> '' and length(FinancialContext) > 16    and ApprovedDate is not null  order by EntityId,ApprovedDate desc";

           
            var cmd = new NpgsqlCommand(query, con);
            var da = new NpgsqlDataAdapter(cmd);

            try
            {
                da.Fill(dt);
                Log.Debug($"GatherDataFromRatingScenario executed successfully at: {DateTime.Now:dd-MM-yyyy H:mm:ss}");
            }
            catch (Exception e)
            {
                Log.Error("GatherDataFromRatingScenario failed : \n" + e.Message + e.StackTrace );
            }

            return dt;
        }

        private static string FindEntityVersion(string financialContext, string entityId)
        {
            int firstChar = financialContext.IndexOf(";") + ";".Length;
            int secondChar = financialContext.LastIndexOf("#");

            var entityVersion = financialContext.Substring(firstChar, secondChar - firstChar);

            int entityVersionIndex = entityVersion.IndexOf('#');
            if (entityVersionIndex > 0)
            {
                entityVersion = entityVersion.Substring(0, entityVersionIndex);
            }

            var entityIdWithVersion = entityId + '|' + entityVersion;
            return entityIdWithVersion;
        }
        
        private static string FindEVersion(string financialContext, string entityId)
        {
            try
            {
                int firstChar = financialContext.IndexOf(";") + ";".Length;
                int secondChar = financialContext.LastIndexOf("#");

                var eVersion = financialContext.Substring(firstChar, secondChar - firstChar);

                int entityVersionIndex = eVersion.IndexOf('#');
                if (entityVersionIndex > 0)
                {
                    eVersion = eVersion.Substring(0, entityVersionIndex);
                }

                var Version = eVersion;
                return Version;
            }
            catch(Exception e)
            {
                Log.Error("Find Entityversion failed:\n" + e.Message + "\n" + e.StackTrace + "\n"  + financialContext + "\n" + entityId);
                return null;
            }
            
        }

        private static int FindFinancialId(string financialContext)
        {
            try
            {
                string FinancialId = financialContext.Substring(0, financialContext.IndexOf(':'));
                return Convert.ToInt32(FinancialId);
            }
            catch (Exception e)
            {
                Log.Error("Find FinancialId failed:\n" + e.Message + "\n" + e.StackTrace + "\n" + financialContext + "\n" );
                return 0;
            }
        }
   
        private static NpgsqlCommand GetFinancialFromHistorical(string firstNumber, string entityVersion, NpgsqlConnection con, string entityid, DateTime date, string FinancialContext, int FinancialId, string sourcepopulateddate, string approveddate,decimal? FlowsCommonShareCapital)
         {
            

            var query = $@"select distinct on(a.pkid_) 
				 d.cdicode as cdi,d.gc18 as afm,coalesce(b.cashandequivalents,0.00)::numeric(19,2) as csh,coalesce(b.ebitda,0.00)::numeric(19,2) as ebitda, 
				coalesce(b.totequityreserves,0.00)::numeric(19,2) as eqty,coalesce(b.goodwill,0.00)::numeric(19,2) as gdwill,coalesce(b.netprofit,0.00)::numeric(19,2) as nt_incm,coalesce(b.salesrevenues,0.00)::numeric(19,2) as sales_revenue,
				 coalesce(b.NetFixedAssets,0.00)::numeric(19,2) as netfixedassets,
				 0::numeric(19,2) as inventory ,
				 0::numeric(19,2) as nettradereceivables,
                coalesce(b.totalassets,0.00)::numeric(19,2) as TotalAssets,
	            coalesce(b.commonsharecapital,0.00)::numeric(19,2) as CommonShareCapital,
				0::numeric(19,2) as TradesPayable,
                0::numeric(19,2) as TotalBankingDebt,
				0::numeric(19,2) as ShortTermBankingDebt,
                0::numeric(19,2) as LongTermBankingDebt,
				coalesce(b.totalliabilities,0.00)::numeric(19,2) as TotalLiabilities,
				coalesce(b.GrossProfit,0.00)::numeric(19,2) as GrossProfit,
                coalesce(b.Ebit,0.00)::numeric(19,2) as Ebit,b.profitbeforetax::numeric(19,2) as ProfitBeforeTax,
			    coalesce(b.workingcapital,0.00)::numeric(19,2) as WorkingCapital,
                coalesce(b.dcfcffrmoperact,0.00)::numeric(19,2) as FlowsOperationalActivity,
                coalesce(b.dcfcffrominvestact,0.00)::numeric(19,2) as FlowsInvestmentActivity,
				coalesce(b.dcfcffromfinact,0.00)::numeric(19,2) as FlowsFinancingActivity,
                NULLIF({FlowsCommonShareCapital},{0.0011})::numeric(19,2)  as ChgCommonShareCapital_ChgSharePremium,
				0::numeric(19,2) as Balancedividendspayable,               
				coalesce(b.grossprofitmargin,0.00)::numeric(19,2) as GrossProfitMargin,
                coalesce(b.netprofitmargin,0.00)::numeric(19,2) as NetProfitMargin,
				coalesce(b.ebitdamargin,0.00)::numeric(19,2) as EbitdaMargin,
				0::numeric(19,2) as TotalBankingDebttoEbitda,
				0::numeric(19,2) as NetBankingDebttoEbitda,
                 coalesce(b.debttoequity,0.00)::numeric(19,2) as TotalLiabilitiestoTotalEquity,
				 coalesce(b.returnonassets,0.00)::numeric(19,2) as ReturnOnAssets,
                 coalesce(b.returnontoteqres,0.00)::numeric(19,2) as ReturnonEquity,
                 coalesce(b.interestcoverage,0.00)::numeric(19,2) as interestcoverage,
                 coalesce(b.currentratio,0.00)::numeric(19,2) as CurrentRatio,
				 coalesce(b.quickratio,0.00)::numeric(19,2) as QuickRatio,
				 a.statementyear::text as fnc_year,
				 to_char(cast(cast(a.statementdatekey_ as varchar(15)) as date),'yyyymmdd') as publish_date,
				 to_char(c.approveddate,'yyyymmdd') as approveddate,'20210930' as reference_date,
	             concat_ws('|',cast(d.entityid as text),cast(d.versionid_ as text)) as entityid,{FinancialId}::int as FinancialId , {firstNumber}::int as Statementid 
                  from olapts.factuphiststmtfinancial a
                                 join olapts.factuphiststmtfinancialgift b on a.pkid_ = b.pkid_ and a.versionid_ = b.versionid_ 
                                 join olapts.factratingscenario c on c.entityid   = cast(a.entityid as int)
                                 join olapts.factentity d on d.entityid  = cast(a.entityid as int)
                                 where a.entityid = '{entityid}' and a.financialid = '{FinancialId}' and a.statementid = '{Convert.ToInt32(firstNumber)}' 
                                 and statementmonths = 12 
                                 and a.sourcepopulateddate_ <= '{sourcepopulateddate}'
                                 and cast( c.ApprovedDate as date) >= '2021-01-06' and cast(c.ApprovedDate as date) <= '{date.ToString("yyyy-MM-dd")}' 
                                 and c.financialcontext = '{FinancialContext}'
                                 and c.ApprovedDate = '{approveddate}' 
                                 and d.versionid_ = '{Convert.ToInt32(entityVersion)}'
                                 and c.modelid = ('FA_FIN')
                                 order by a.pkid_, a.sourcepopulateddate_ desc";
            
            var cmd1 = new NpgsqlCommand(query, con);
            return cmd1;
            
        }
        
        private static void ExportToCsv(DataTable dataTable, string filePath, DateTime date)
        {

            var path = $@"{filePath}\RM_REPORT_{date.ToString("yyyyMMdd").Replace("/", "")}.csv";

            if (File.Exists(path))
            {
                File.Delete(path);
            }

            using (StreamWriter sw = (File.Exists(path) ? File.AppendText(path) : File.CreateText(path)))
            {
                for (int i = 0; i < dataTable.Columns.Count; i++)
                {
                    sw.Write(dataTable.Columns[i]);

                    if (i < dataTable.Columns.Count - 1 )
                        sw.Write(";");
                }
                //
                sw.Write(sw.NewLine);

                foreach (DataRow row in dataTable.Rows)
                {
                    for (int i = 0; i < dataTable.Columns.Count; i++)
                    {
                        if (!Convert.IsDBNull(row[i]))
                        {
                            string value = row[i].ToString();
                            if (value.Contains(";"))
                            {
                                value = String.Format("\"{0}\"", value);
                                sw.Write(value);
                            }
                            else
                            {
                                sw.Write(row[i].ToString());
                            }
                        }

                        if (i < dataTable.Columns.Count - 1)
                            sw.Write(";");
                    }
                    sw.Write(sw.NewLine);
                }
                Log.Debug($"CSV export finished at : {DateTime.Now:dd-MM-yyyy H:mm:ss}");
                sw.Close();
            }
        }

        public static void ExportResultsTxt(string result)
        {
            var path = $@"{Path}\RM_Reports_Result_{DateTime.Now.ToShortDateString().Replace("/", "-")}_{DateTime.Now.Hour.ToString()}_{DateTime.Now.Minute.ToString()}.txt";

            using (StreamWriter sw = (File.Exists(path) ? File.AppendText(path) : File.CreateText(path)))
            {
                sw.Write(result);
                Log.Debug($"Results.txt export finished at : {DateTime.Now:dd-MM-yyyy H:mm:ss}");
                sw.Close();
            }
        }
    }
}
