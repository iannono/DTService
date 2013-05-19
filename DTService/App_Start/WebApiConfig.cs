using System;
using System.Collections.Generic;
using System.Linq;
using System.Web.Http;

namespace DTService
{
    public static class WebApiConfig
    {
        public static void Register(HttpConfiguration config)
        {
            config.Routes.MapHttpRoute(
                name: "DefaultApi",
                routeTemplate: "api/{controller}/{table}",
                defaults: new { table = RouteParameter.Optional }
            );
        }
    }
}
