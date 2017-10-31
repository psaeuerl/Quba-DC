using QubaDC.CRUD;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;


namespace QubaDC.Evaluation
{
    public class InsertPhase
    {

        public int Inserts { get; internal set; }
        public bool dropDb { get; internal set; }

        internal InsertPhaseResult runFor(QubaDCSystem quba, String dbname, int Sections)
        {
            List<long> insertValues = new List<long>();
            QubaDC.CRUD.InsertOperation[] inserts = InsertGenerator.GenerateFor(1, Inserts, dbname, Sections);
            Stopwatch sw = new Stopwatch();

            int cnt = 0;
            foreach (var insert in inserts)
            {

                sw.Start();
                quba.CRUDHandler.HandleInsert(insert);
                sw.Stop();
                insertValues.Add(sw.ElapsedMilliseconds);
                sw.Reset();
                cnt++;
                if (cnt % 1000 == 0)
                    Console.WriteLine("Inserted " + cnt);
            }
            long sum = insertValues.Sum();
            return new InsertPhaseResult()
            {
                insertTimes = insertValues.ToArray(),
                insertSum = insertValues.Sum()
            };
            
        }
    }
}
