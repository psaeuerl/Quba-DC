using QubaDC.SMO;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QubaDC.Integrated.SMO
{
    public class IntegratedSMOExecuter
    {
        public static void Execute(SMORenderer SMORenderer, DataConnection con, string[] PreLockingStatements, string[] AfterLockingStatemnts, string[] tablesToLock)
        {
            String lockTables = SMORenderer.CRUDRenderer.RenderLockTables(tablesToLock).AsScript();
            String result = String.Join(System.Environment.NewLine,
                SMORenderer.CRUDRenderer.RenderAutoCommitZero().AsScript(),
                PreLockingStatements.AsScript(),
                lockTables,
                AfterLockingStatemnts.AsScript(),
                SMORenderer.CRUDRenderer.RenderCommitAndUnlock().AsScript()
                );
            con.AquiereOpenConnection(c =>
            {
                try
                {

                    con.ExecuteSQLScript(result, c);
                }
                catch (Exception e)
                {
                    String[] rollbackAndUnlock = SMORenderer.CRUDRenderer.RenderRollBackAndUnlock();
                    con.ExecuteNonQuerySQL(rollbackAndUnlock[0], c);
                    con.ExecuteNonQuerySQL(rollbackAndUnlock[1], c);
                    throw new InvalidOperationException("Got exception after Table Locks, rolled back and unlocked", e);
                }
            });
        }
    }
}
