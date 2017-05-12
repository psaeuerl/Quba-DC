using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using QubaDC.CRUD;
using QubaDC.Utility;

namespace QubaDC.Separated
{
    public class SeparatedQSSelectHandler : QueryStoreSelectHandler
    {
        public override void HandleSelect(SelectOperation s, SchemaManager manager, DataConnection con)
        {
            //3.Open transaction and lock tables(I)
            //4.Execute(original) query and retrieve subset.
            //5.Assign the last global update timestamp to the query (R7).
            //6.Close transaction and unlock tables(A) 

            //PS
            //We do not need to lock any tables here as we are querying on the historic database
            //If we want to, we COULD lock and do the query like the others
            //We choose here not to lock

            //Question is ... if we do not lock what is global update timestamp?
            //We query the global update timestampt and get the max from it
            //Then we get which schema was active there and get the history tables for this one            


            //What to do here:
            //a.) copy the operation
            var newOperation = JsonSerializer.CopyItem(s);
            //b.) change all tables to the respective history ones
            foreach(var selectedTable in newOperation.GetAllSelectedTables())
            {
                
            }
            //c.) add timestamp parts to it
            //d.) execute it and return
            throw new NotImplementedException();
        }
    }
}
