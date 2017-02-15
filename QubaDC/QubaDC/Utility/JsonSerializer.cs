using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QubaDC.Utility
{
    public class JsonSerializer
    {

        public static String SerializeObject(Object o)
        {
            string output = JsonConvert.SerializeObject(o);
            return output;
        }

        public static T DeserializeObject<T>(String obj)
        {
            T result = JsonConvert.DeserializeObject<T>(obj);
            return result;
        }
    }
}
