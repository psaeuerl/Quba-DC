using QubaDC.CRUD;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;


namespace QubaDC.Evaluation
{
    public class Phase
    {
        internal int phaseNumber;

        public Boolean DoDelets { get; set;}
        public int Deletes { get; internal set; }
        public int Inserts { get; internal set; }
        public Boolean DoUPdates { get; set; }
        public int Updates { get; internal set; }



        internal PhaseResult runFor(QubaDCSystem quba, String dbname)
        {
            List<long> insertValues = new List<long>();
            QubaDC.CRUD.InsertOperation[] inserts = InsertGenerator.GenerateFor(phaseNumber, Inserts, dbname);
            Stopwatch sw = new Stopwatch();
            foreach (var insert in inserts)
            {
                sw.Start();
                quba.CRUDHandler.HandleInsert(insert);
                sw.Stop();
                insertValues.Add(sw.ElapsedMilliseconds);
                sw.Reset();
            }
            long sum = insertValues.Sum();
            if (DoDelets)
                throw new NotImplementedException("NI DeletePhase");
            if (DoUPdates)
                throw new NotImplementedException("NI UpdatePhase");

            //TODO NO_CACHE + FLUSH!
            //quba.DataConnection.
            //SelectOperation getCurrentcSelect = 


            return new PhaseResult()
            {
                insertTimes = insertValues.ToArray(),
                insertSum = insertValues.Sum()
            };
            
        }
    }
}
