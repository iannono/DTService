using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Web.Http;
using System.Data;
using DTService.Handlers;

namespace DTService.Controllers
{
    
    public class DataServiceController : ApiController
    {
        private FileHandler fileHandler = new FileHandler();
        // GET api/dataservice
        public IEnumerable<string> Get()
        {
            return new string[] { "value1", "value2" };
        }

        // GET api/dataservice/pincome
        public string Get(string table)
        {
            string results = fileHandler.ImportFile((TableName)fileHandler.GetTableTypeFromTableName(table));
            return results;
        }

        // POST api/dataservice
        public void Post([FromBody]string value)
        {
        }

        // PUT api/dataservice/5
        public void Put(int id, [FromBody]string value)
        {
        }

        // DELETE api/dataservice/5
        public void Delete(int id)
        {
        }
    }
}
