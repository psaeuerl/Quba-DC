﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using QubaDC.CRUD;
using QubaDC.Utility;
using QubaDC.Restrictions;
using System.Data;
using QubaDC.DatabaseObjects;

namespace QubaDC.Separated
{
    public class SeparatedQSSelectHandler : QueryStoreSelectHandler
    {
        public override QueryStoreSelectResult HandleSelect(SelectOperation s, SchemaManager schemaManager, DataConnection con, TableMetadataManager metaManager, CRUDVisitor cRUDHandler,QueryStore qs)
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


            //f.) Execute it
            //DataTable normResult = null;
            //DataTable hashTable = null;
            //String hash = null;
            //Guid guid = Guid.NewGuid();
            //String time = cRUDHandler.CRUDRenderer.SerializeDateTime(queryTime);
            //con.DoTransaction((trans, c) =>
            //{
            //    normResult = con.ExecuteQuery(select, c);
            //    hashTable = con.ExecuteQuery(selectHash, c);
            //    hash = hashTable.Select().First().Field<String>(0);
            //    String insert = qs.RenderInsert(originalrenderd, originalSerialized, RewrittenSerialized, select, time, hash, guid,selectHash, selectHashSerialized,null);
            //   long? id = con.ExecuteInsert(insert, c);
            //    System.Diagnostics.Debug.WriteLine(id.Value);
            //    trans.Commit();
            //});
            SchemaInfo info = schemaManager.GetCurrentSchema();
            var currentSchema = info.Schema;
            TableToLock[] aliasTables = s.GetAllSelectedTables()                                             
                                        .Select(x => {
                                            var histTable = currentSchema.FindHistTable(x);
                                            var res = new TableToLock { Alias = x.TableAlias, LockAsWrite = false, Name = cRUDHandler.CRUDRenderer.PrepareTable(histTable.ToTable()) };
                                            return res;

                                         }).ToArray();
            TableToLock[] metaTables = s.GetAllSelectedTables().Select(x => metaManager.GetMetaTableFor(x.TableSchema, x.TableName))
                                                               .Select(x => new TableToLock { Alias = null, LockAsWrite = false, Name = cRUDHandler.CRUDRenderer.PrepareTable(x) })
                                                               .ToArray();

            TableToLock[] tablesToLock = aliasTables.Union(metaTables).ToArray();
            Table[] MetaTables = s.GetAllSelectedTables()//.Select(x => metaManager.GetMetaTableFor(x.TableSchema, x.TableName))
                    .ToArray();


            Dictionary<String, Guid?> table_to_ids = null;
            DateTime queryTime = DateTime.Now;
            String originalrenderd = "";
            String originalSerialized = "";
            String RewrittenSerialized = "";
            String select = "";
            String selectHash = "";
            String selectHashSerialized = "";
            SelectOperation newOperation = null;
            DataTable normResult = null;

            Func<SchemaInfo, DateTime, String[]> RenderSelectStatements = (schema, dt) =>
            {
                queryTime = dt;
                //a.) copy the operation
                newOperation = JsonSerializer.CopyItem(s);
                //b.) change all tables to the respective history ones + build restrictions for it
                List<Restriction> TimeStampRestrictions = new List<Restriction>();
                SchemaInfo SchemaInfo = schema;
                foreach (var selectedTable in newOperation.GetAllSelectedTables())
                {
                    var histTable = SchemaInfo.Schema.FindHistTable(selectedTable).ToTable();
                    selectedTable.TableName = histTable.TableName;
                    selectedTable.TableSchema = histTable.TableSchema;
                    OperatorRestriction startTs = new OperatorRestriction()
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

                //d.) render it and return



                 originalSerialized = JsonSerializer.SerializeObject(s);
                 originalrenderd = cRUDHandler.RenderSelectOperation(s);

                 select = cRUDHandler.RenderSelectOperation(newOperation);
                 RewrittenSerialized = JsonSerializer.SerializeObject(newOperation);

                 selectHash = cRUDHandler.RenderHashSelect(newOperation);
                 selectHashSerialized = JsonSerializer.SerializeObject(newOperation);
                return new String[] { selectHash, select };
            };
            String hash = null;
            Guid guid = Guid.NewGuid();
            String time = cRUDHandler.CRUDRenderer.SerializeDateTime(queryTime);
            Func<DataTable, DataTable, String> RenderInsert = (hashTable, SelectTable) =>
            {
                normResult = SelectTable;
                hash = hashTable.Select().First().Field<String>(0);
                String additionalInfos = JsonSerializer.SerializeObject(table_to_ids);

                String insert = qs.RenderInsert(originalrenderd, originalSerialized, RewrittenSerialized, select, time, hash, guid, selectHash, selectHashSerialized, additionalInfos);
                return insert;
            };

            Action<String> log = (logst) => { System.Diagnostics.Debug.WriteLine(logst); };
            SeparatedQSSelectExecuter executer = new SeparatedQSSelectExecuter();
            long id = executer.ExecuteStatementsOnLockedTables(RenderInsert, RenderSelectStatements, tablesToLock, con, cRUDHandler.CRUDRenderer, schemaManager, info, MetaTables, metaManager, log);


            var execResult = new QueryStoreSelectResult()
            {
                RewrittenSerialized = JsonSerializer.SerializeObject(newOperation),
                RewrittenRenderd = select,
                Result = normResult,
                TimeStampOfExecution = queryTime
                ,
                Hash = hash
                ,
                GUID = guid

            };

            return execResult;
        }

        public override QueryStoreReexecuteResult ReExecuteSelectFor(Guid gUID, QueryStore qs,DataConnection con, CRUDVisitor cRUDHandler, SchemaManager schemaManager)
        {
            String selectQueryStoreROw = qs.RenderSelectForQueryStore(gUID);
            DataTable t = con.ExecuteQuery(selectQueryStoreROw);
            if (t.Rows.Count != 1)
                throw new InvalidOperationException("Expected to get 1 Row, got: " + t.Rows.Count + " Could not do rexecue for guid: " + gUID.ToString());
            var row = t.Select().First();

            String hash = row.Field<String>("Hash");
            String query = row.Field<String>("ReWrittenQuery");
            String querySerialized = row.Field<String>("HashSelect");

            DataTable normaResult = null;
            DataTable hashTable = null;
            con.DoTransaction((trans, c) =>
            {

                hashTable = con.ExecuteQuery(querySerialized, c);
                String currentHash = hashTable.Select().First().Field<String>(0);
                if (currentHash != hash)
                    throw new InvalidOperationException("Hashes for GUID: " + gUID.ToString() + " Are not equal, HashSelect: "+querySerialized+System.Environment.NewLine+ "Select: "+query);
                normaResult = con.ExecuteQuery(query, c);
                trans.Commit();
            });

            return new QueryStoreReexecuteResult()
            {
                GUID = gUID,
                Hash = hash,
                Result = normaResult
            };
        }
    }
}
