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
        private static JsonSerializerSettings settings = new JsonSerializerSettings { TypeNameHandling = TypeNameHandling.All };
    public static String SerializeObject(Object o)
        {
            string output = JsonConvert.SerializeObject(o,settings);
            return output;
        }

        public static T DeserializeObject<T>(String obj)
        {
            object result = JsonConvert.DeserializeObject(obj, settings);
            return (T)result;
        }

        public static T CopyItem<T>(T toCopy)
        {
            String s = SerializeObject(toCopy);
            T result = DeserializeObject<T>(s);
            return result;
        }
    }
}
