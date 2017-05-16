using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using QubaDC.CRUD;
using QubaDC.Utility;
using QubaDC.Restrictions;
using System.Data;

namespace QubaDC.Separated
{
    public class SeparatedQSSelectHandler : QueryStoreSelectHandler
    {
        public override QueryStoreSelectResult HandleSelect(SelectOperation s, SchemaManager manager, DataConnection con, GlobalUpdateTimeManager timemanager, CRUDVisitor cRUDHandler,QueryStore qs)
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
            //0.) Get last updated Timestamp
            var lastGlobalUpdate =  timemanager.GetLatestUpdate();
            var queryTime = lastGlobalUpdate.DateTime;

            //a.) copy the operation
            var newOperation = JsonSerializer.CopyItem(s);
            //b.) change all tables to the respective history ones + build restrictions for it
            List<Restriction> TimeStampRestrictions = new List<Restriction>();
            SchemaInfo SchemaInfo = manager.GetSchemaActiveAt(lastGlobalUpdate.DateTime);
            foreach(var selectedTable in newOperation.GetAllSelectedTables())
            {
                var histTable = SchemaInfo.Schema.FindHistTable(selectedTable);
                selectedTable.TableName = histTable.TableName;
                selectedTable.TableSchema = histTable.TableSchema;
                OperatorRestriction  startTs = new OperatorRestriction()
                {
                    LHS = new ColumnOperand()
                    {
                         Column = new ColumnReference()
                         {
                              ColumnName = SeparatedConstants.StartTS,
                               TableReference = selectedTable.TableAlias
                         }
                    },
                    Op = RestrictionOperator.LET
                    ,
                    RHS = new DateTimeRestrictionOperand()
                    {
                          Value = queryTime
                    },                    
                };

                OperatorRestriction endTsLt = new OperatorRestriction()
                {
                    LHS = new DateTimeRestrictionOperand()
                    {
                        Value = queryTime
                    },
                    Op = RestrictionOperator.LT
    ,
                    RHS = new ColumnOperand()
                    {
                        Column = new ColumnReference()
                        {
                            ColumnName = SeparatedConstants.EndTS,
                            TableReference = selectedTable.TableAlias
                        }
                    },
                };
                OperatorRestriction endTSNull = new OperatorRestriction()
                {
                    LHS = new ColumnOperand()
                    {
                        Column = new ColumnReference()
                        {
                            ColumnName = SeparatedConstants.EndTS,
                            TableReference = selectedTable.TableAlias
                        }
                    },
                    Op = RestrictionOperator.IS
,
                    RHS = new LiteralOperand()
                    {
                        Literal = "NULL"
                    }
                };
                var OrRestriction = new OrRestriction();
                OrRestriction.Restrictions = new Restriction[] { endTsLt, endTSNull };
                var AndRestriction = new AndRestriction();
                AndRestriction.Restrictions = new Restriction[] { startTs, OrRestriction };
                TimeStampRestrictions.Add(AndRestriction);
            }
            //c.) add timestamp parts to it
            if (newOperation.Restriction != null)
                TimeStampRestrictions.Add(newOperation.Restriction);

            newOperation.Restriction = new AndRestriction() { Restrictions = TimeStampRestrictions.ToArray() };

            //c.2) add the hash-column to it
            newOperation.RenderHashColumn = true;

            //d.) render it and return

            String select= cRUDHandler.RenderSelectOperation(newOperation);

            String originalSerialized = JsonSerializer.SerializeObject(s);
            String originalrenderd = cRUDHandler.RenderSelectOperation(s);

            String RewrittenSerialized = JsonSerializer.SerializeObject(newOperation);

            String selectHash = cRUDHandler.RenderHashSelect(newOperation);

            //f.) Execute it
            DataTable normResult = null;
            DataTable hashTable = null;
            String hash = null;
            Guid guid = Guid.NewGuid();
            String time = cRUDHandler.CRUDRenderer.SerializeDateTime(queryTime);
            con.DoTransaction((trans, c) =>
            {
                normResult = con.ExecuteQuery(select, c);
                hashTable = con.ExecuteQuery(selectHash, c);
                hash = hashTable.Select().First().Field<String>(0);
                String insert = qs.RenderInsert(originalrenderd, originalSerialized, RewrittenSerialized, select, time, hash, guid);
                con.ExecuteInsert(insert, c);
            });

            hash = hashTable.Select().First().Field<String>(0);
            var execResult = new QueryStoreSelectResult()
            {
                RewrittenSerialized = JsonSerializer.SerializeObject(newOperation),
                RewrittenRenderd = select,
                Result = normResult,
                TimeStampOfExecution = queryTime
                , Hash = hash
                , GUID = guid

            };
            return execResult;
        }

    }
}
