using QubaDC.CRUD;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QubaDC.Integrated.CRUD
{
    public class IntegratedCRUDExecuter
    {
        public static void ExecuteStatementsOnLockedTables(Func<String[]> RenderStatements, String[] locktables, DataConnection DataConnection, CRUDRenderer crudRenderer)
        {
            List<String> parts = new List<string>();
            parts.AddRange(crudRenderer.RenderLockTables(locktables));
            parts.AddRange(RenderStatements());
            parts.AddRange(crudRenderer.RenderCommitAndUnlock());
            String[] toJoin = parts.Select(x => x.Last() == ';' ? x : x + ";").ToArray();
            String script = String.Join(System.Environment.NewLine, toJoin);

            DataConnection.AquiereOpenConnection(con =>
            {
                try
                {
                    DataConnection.ExecuteSQLScript(script, con);
                }
                catch (Exception e)
                {
                    String[] rollbackAndUnlock = crudRenderer.RenderRollBackAndUnlock();
                    DataConnection.ExecuteNonQuerySQL(rollbackAndUnlock[0]);
                    DataConnection.ExecuteNonQuerySQL(rollbackAndUnlock[1]);
                    throw new InvalidOperationException("Got exception after Table Locks, rolled back and unlocked", e);
                }

                //String[] lockTableStatements = crudRenderer.RenderLockTables(locktables);
                //try
                //{
                //    foreach (var setupSql in lockTableStatements)
                //        DataConnection.ExecuteNonQuerySQL(setupSql, con);
                //}
                //catch (Exception e)
                //{
                //    throw new InvalidOperationException("Could not aquire locks for:" + String.Join(",", locktables), e);
                //}

                //try
                //{
                //    String[] statements = RenderStatements();
                //    String[] success = crudRenderer.RenderCommitAndUnlock();
                //    foreach (var stmt in statements)
                //        DataConnection.ExecuteSQLScript(stmt, con);
                //    DataConnection.ExecuteNonQuerySQL(success[0], con);
                //    DataConnection.ExecuteNonQuerySQL(success[1], con);

                //}
                //catch (Exception e)
                //{
                //    String[] rollbackAndUnlock = crudRenderer.RenderRollBackAndUnlock();
                //    DataConnection.ExecuteNonQuerySQL(rollbackAndUnlock[0]);
                //    DataConnection.ExecuteNonQuerySQL(rollbackAndUnlock[1]);
                //    throw new InvalidOperationException("Got exception after Table Locks, rolled back and unlocked", e);
                //}

            });
        }
    }
}
