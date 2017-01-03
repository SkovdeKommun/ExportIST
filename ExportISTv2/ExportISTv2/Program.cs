using System;
using System.Data.SqlClient;
using System.IO;
using System.Text;

namespace ExportIST
{
   class Program
   {
      static void Main(string[] args)
      {

         string dbServer = "";
         string dbUser = "";
         string dbPwd = "";
         string dbDBase = "";

         string idag = DateTime.Today.ToString("yyyyMMdd");
         string lasar_start = "";
         string lasar_end = "";

         if (args.Length > 0)
         {
            foreach (string arg in args)
            {
               if (arg.Length > 4)
               {
                  string a = arg.ToLower().Substring(0, 4);
                  switch (a)
                  {
                     case "srv=": dbServer = arg.Substring(4); break;
                     case "usr=": dbUser = arg.Substring(4); break;
                     case "pwd=": dbPwd = arg.Substring(4); break;
                     case "sta=": lasar_start = arg.Substring(4); break;
                     case "end=": lasar_end = arg.Substring(4); break;
                     case "dat=": dbDBase = arg.Substring(4); break;
                     default:
                        Console.WriteLine("Dont know {0}", a);
                        Console.WriteLine("Usage: ExportIST [srv={servername}] [usr={db userid}] [pwd={db pwd}] [dat={database}] [sta={period startdate}] [end={period endate}]");
                        return;

                  }
               }

            }
         }

         if (dbUser == "" || dbPwd == "" || dbServer == "" || dbDBase == "")
         {
            Console.WriteLine("Du måste ange anslutningsinformation till databasen, såsom användarnamn, lösenord och databasnamn!?`n");

            Console.WriteLine("Usage: ExportIST [srv={servername}] [usr={db userid}] [pwd={db pwd}] [dat={database}] [sta={period startdate}] [end={period endate}]");
            return;

         }

         string connectionString = String.Format("user id={0};password={1};server={2};database={3}", dbUser, dbPwd, dbServer, dbDBase);

         FileInfo t;
         StreamWriter ut;
         SqlConnection cn;
         SqlCommand cmPer;


         t = new FileInfo("ExportIST.csv");

         ut = new StreamWriter(File.Open("ExportIST.csv", FileMode.Create), Encoding.GetEncoding("iso-8859-1"));
         cn = new SqlConnection(connectionString);

         try
         {
            cn.Open();
         }
         catch (Exception e)
         {
            Console.WriteLine("Error! Exception " + e.Message);
            return;
         }

         string qry_prefix = String.Format("[{0}].[{0}]", dbDBase);

         // Do we need to obtain starting and ending period
         if (lasar_start == "" || lasar_end == "")
         {
            string SQL_Period = String.Format(@"SELECT DISTINCT spd.FROMDAT, spd.TOMDAT FROM {0}.[kjgp0spd] spd WHERE (spd.FROMDAT < '{1}' AND TOMDAT >= '{1}' AND PERIODTYP = 'LASAR')", qry_prefix, idag);
            cmPer = new SqlCommand(SQL_Period, cn);

            try
            {
               SqlDataReader rdPer = cmPer.ExecuteReader();
               if (rdPer.Read())
               {
                  lasar_start = rdPer[0].ToString();
                  lasar_end = rdPer[1].ToString();
               }
               else
               {
                  rdPer.Close();

                  // Vi verkar vara utanför perioden. Sök senast läsårdsperiod
                  string SQL_Period2 = String.Format(@"SELECT DISTINCT TOP 1 FROMDAT, TOMDAT FROM {0}.[kjgp0spd] WHERE (PERIODTYP = 'LASAR' AND TOMDAT <= '{0}') ORDER BY FROMDAT desc, TOMDAT", qry_prefix, idag);
                  cmPer = new SqlCommand(SQL_Period2, cn);
                  try
                  {
                     rdPer = cmPer.ExecuteReader();
                     if (rdPer.Read())
                     {
                        lasar_start = rdPer[0].ToString();
                        lasar_end = rdPer[1].ToString();
                     }
                     else
                     {
                        Console.WriteLine("Kunde inte läsa ut period för läsår!?");
                        Console.WriteLine("SQL: {0}", SQL_Period);
                        cn.Close();
                        return;
                     }
                  }
                  catch (Exception pE0)
                  {
                     Console.WriteLine("Read Last Period Exception: " + pE0.Message);
                  }
               }

               rdPer.Close();
            }
            catch (Exception pE)
            {
               Console.WriteLine("Read Period Exception: " + pE.Message);
            }
         }

         Console.WriteLine("Läsår start {0}, Läsår slut {1}", lasar_start, lasar_end);

         string SQL = @"
select distinct ekl.PERSONNR, sre.SKOLENHETSKOD, per.FORNAMN, per.EFTERNAMN, ekl.PROGRAM, ekl.KLASS, ekl.ARSKURS, ekl.FROMDAT, ekl.TOMDAT, per.hemkommun
from            {0}.[KJ1P0PER] per
inner join      {0}.[kjgp0ekl] ekl on per.personnr=ekl.personnr
inner join      {0}.[kjgp0sko] sko on (ekl.skolenhet=sko.skolenhet)
left join       {0}.[kjgp0sre] sre on (ekl.rektorsenhet=sre.rektorsenhet)
inner join      {0}.[kjgp0spd] spd on (ekl.period=spd.period and spd.AKTIV=1)
left join       {0}.[kj1p0plk] plk on per.personnr=plk.personnr
left join       {0}.[kjgp0skd] skd on (per.hemkommun=skd.kod and skd.KODTYP = 'KOMMUN')

Where ((ekl.FROMDAT >= '{1}' AND ekl.TOMDAT <= '{2}'))";
         SqlCommand cm = new SqlCommand(String.Format(SQL, qry_prefix, lasar_start, lasar_end), cn);

         //Console.WriteLine("Getting data for {0} - {1}", lasar_start, lasar_end);

         ut.WriteLine("PERSONNR;SEKEL;SKOLENHETSKOD;FORNAMN;EFTERNAMN;PROGRAMKOD;PROGRAMVARIANT;KLASS;ARSKURS;STARTDATUM;SLUTDATUM;PLACERINGSID;HEMKOMMUN;MODERSMAL;SLUTBETYGSRADGR");
         int antal = 0;

         try
         {
            SqlDataReader rd = cm.ExecuteReader();
            while (rd.Read())
            {
               string pnum = rd[0].ToString();
               pnum = pnum.Substring(0, 8) + "-" + pnum.Substring(8);
               string sekel = pnum.Substring(0, 2);
               pnum = pnum.Substring(2);
               string rektorsenhet = rd[1].ToString();
               string fornamn = rd[2].ToString();
               string efternamn = rd[3].ToString();
               string program = rd[4].ToString();
               string programvar = "";
               string klass = rd[5].ToString();
               string arskurs = rd[6].ToString();
               string fromdat = rd[7].ToString();
               string tomdat = rd[8].ToString();
               string placeringsid = "";
               string hemkommun = rd[9].ToString();
               string modersmal = "";
               string slutbetygsradgr = "";
               hemkommun = "";

               //                    if (hemkommun.Length == 9 && hemkommun.Substring(5) != "9999") {
               //                        hemkommun = hemkommun.Substring(5);
               //                    }
               //                    else
               //                    {
               //                       Console.WriteLine("Hemkommunformat okänt {0} för pnum {1}", hemkommun, pnum);
               //                        hemkommun = "";
               //                    }

               fromdat = fromdat.Substring(0, 4) + "-" + fromdat.Substring(4, 2) + "-" + fromdat.Substring(6, 2);
               tomdat = tomdat.Substring(0, 4) + "-" + tomdat.Substring(4, 2) + "-" + tomdat.Substring(6, 2);

               if (fornamn.Length > 30) { fornamn = fornamn.Substring(0, 30); }
               if (efternamn.Length > 120) { efternamn = efternamn.Substring(0, 120); }

               if (pnum != "")
               {
                  ut.WriteLine(pnum + ";" + sekel + ";" + rektorsenhet + ";" + fornamn + ";" + efternamn + ";" + program + ";" + programvar + ";" + klass + ";" + arskurs + ";" + fromdat + ";" + tomdat + ";" + placeringsid + ";" + hemkommun + ";" + modersmal + ";" + slutbetygsradgr);
                  //Console.WriteLine(pnum + ";" + sekel + ";" + rektorsenhet + ";" + fornamn + ";" + efternamn + ";" + program + ";" + programvar + ";" + klass + ";" + arskurs + ";" + fromdat + ";" + tomdat + ";" + placeringsid + ";" + hemkommun + ";" + modersmal + ";" + slutbetygsradgr);
                  antal++;
                  ut.Flush();
               }

            }
         }
         catch (Exception rE)
         {
            Console.WriteLine("Reader error: " + rE.Message);
         }
         finally
         {
            Console.WriteLine("{0} poster skrivna till filen", antal);
            cn.Close();
            ut.Close();
         }
      }
   }
}
