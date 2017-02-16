using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using QubaDC.SMO;
using QubaDC.Utility;

namespace QubaDC.DatabaseObjects
{
    public class Schema
    {
        public IEnumerable<Table> Tables { get { return this._Tables; } }
        public IEnumerable<Table> HistTables { get { return this._HistTables; } }

        private List<Table> _Tables { get ; set; } = new List<Table>();

        private List<Table> _HistTables { get;  set; } = new List<Table>();      

        public void AddTable(Table table, Table histequivalent)
        {
            this._Tables.Add(table);
            this._HistTables.Add(histequivalent);
            //PS Not sure if i should check here also that the columns got more
            Guard.StateTrue(table.Name == histequivalent.Name + "_hist", "Table name does not equal histname_hist");
        }
    }
}
