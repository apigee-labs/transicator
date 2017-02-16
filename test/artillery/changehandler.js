//
// changehandler.js
//
module.exports = {
  InsertBulkDataToDBScope : InsertBulkDataToDBScope,
  InsertBulkDataToDB: InsertBulkDataToDB,
  InsertScopeInRequest: InsertScopeInRequest,
  ValidateScopeFromCS: ValidateScopeFromCS
}

function InsertBulkDataToDBScope(requestParams, context, ee, next) {
   var statusArray = [];

   var scope = makeid()
   // Add some rows (values)
   for (var i = 0; i< 100; i++) {
        var status = {
            "column1": makeid(),
            "column2": "Value" + i,
            "column3": "Value" + i,
            "column4": "Value" + i,
            "column5": "Value" + i,
            "_change_selector": scope
        };
        statusArray.push(status);
   }
   requestParams.body = statusArray;
   requestParams.json = true;
   context.vars.scope = scope
   return next(); // MUST be called for the scenario to continue
}

function InsertBulkDataToDB(requestParams, context, ee, next) {
   var statusArray = [];

   // Add some rows (values)
   for (var i = 0; i< 100; i++) {
        var status = {
            "column1": makeid(),
            "column2": "Value" + i,
            "column3": "Value" + i,
            "column4": "Value" + i,
            "column5": "Value" + i,
            "_change_selector": context.vars.scope
        };
        statusArray.push(status);
   }
   requestParams.body = statusArray;
   requestParams.json = true;
   return next(); // MUST be called for the scenario to continue
}

function InsertScopeInRequest(requestParams, context, ee, next) {
   
   var qstr = {scope: context.vars.scope, limit: 1000}
   requestParams.qs = qstr
   console.log(requestParams.qs);
   return next(); // MUST be called for the scenario to continue
}

function ValidateScopeFromCS(requestParams, response, context, ee, next) {
   // console.log(response.body);
   return next(); // MUST be called for the scenario to continue
}


function makeid()
{
    var text = "";
    var possible = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

    for( var i=0; i < 10; i++ )
        text += possible.charAt(Math.floor(Math.random() * possible.length));

    return text;
}


