package aurora

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/rdsdataservice"
	"github.com/mmatagrin/ctxerror"
	"io/ioutil"
	"os"
	"reflect"
	"strings"
)

type QueryResult map[string]interface{}

var awsSession *session.Session

func SetAwsSession(sess session.Session) {
	awsSession = &sess
}

func ExecuteFile(filePath, separator string, connexion AuroraConnexion) (e ctxerror.CtxErrorTraceI){
	context := ctxerror.SetContext(map[string]interface{}{})

	file, err := os.Open(filePath)
	if err != nil{
		return context.Wrap(err, "unable to open source file")
	}

	sqlQueriesBytes, err := ioutil.ReadAll(file)
	if err != nil {
		return context.Wrap(err, "unable to read source file")
	}

	sqlQueries := string(sqlQueriesBytes)


	transaction , err := BeginTransaction(connexion)
	if err != nil {
		return context.Wrap(err, "enable to start database transaction")
	}

	defer func() {
		if e != nil {
			errRollback := RollbackTransaction(connexion, transaction)
			if errRollback != nil {
				e = e.AddError(errRollback, "unable to rollback transaction")
			}

			return
		}

		errCommit := CommitTransaction(connexion, transaction)
		if errCommit != nil {
			e = ctxerror.Wrap(errCommit, "unable to commit transaction")
		}
	}()

	for _, query := range strings.SplitAfter(sqlQueries, separator) {
		if query == "" {
			continue
		}

		_, err = PerformAuroraQuery(query, nil, connexion, &transaction)

		if err != nil {
			context.AddContext("current_query", string(query))
			return context.Wrap(err, "unable to perform query")
		}
	}

	return nil
}

func PerformAuroraQuery(query string, parameters map[string]interface{}, connexion AuroraConnexion, transactionId *string) (*rdsdataservice.ExecuteStatementOutput, error) {
	context := ctxerror.SetContext(map[string]interface{}{
		"query": query,
		"parameters": parameters,
	})

	if awsSession == nil {
		return nil, context.New("aws session is nil")
	}

	rdsClient := rdsdataservice.New(awsSession)

	var sqlParams []*rdsdataservice.SqlParameter
	if parameters != nil && len(parameters) != 0 {
		sqlParams = make([]*rdsdataservice.SqlParameter, 0, len(parameters))
		for key, value := range parameters {
			field, err := valueToRdsField(value)
			if err != nil {
				context.AddContext("current_parameter", []interface{}{key, value})
				return nil, context.Wrap(err, "error converting parameter to Rds field")
			}
			param := &rdsdataservice.SqlParameter{
				Name: aws.String(key),
				Value: field,
			}
			sqlParams = append(sqlParams, param)
		}
	}

	executeStatementInput := rdsdataservice.ExecuteStatementInput{
		Database: aws.String(connexion.Database),
		ResourceArn: aws.String(connexion.ResourceArn),
		SecretArn: aws.String(connexion.SecretArn),
		Sql: aws.String(query),
		Parameters: sqlParams,
	}

	if transactionId != nil {
		executeStatementInput.TransactionId = transactionId
	}

	res, err := rdsClient.ExecuteStatement(&executeStatementInput)

	if err != nil {
		if strings.Contains(err.Error(), "Communications link failure") {
			return nil, context.Wrap(err, "RDS database is still waking up, try again in a minute")
		} else {
			return nil, context.Wrap(err, "error executing query")
		}
	}

	return res, nil


}

func PerformAuroraQueries(query string, parameters []map[string]interface{}, connexion AuroraConnexion, transactionId *string) (*rdsdataservice.BatchExecuteStatementOutput, error) {
	context := ctxerror.SetContext(map[string]interface{}{
		"query": query,
		"parameters": parameters,
	})

	if awsSession == nil {
		return nil, context.New("aws session is nil")
	}

	rdsClient := rdsdataservice.New(awsSession)

	var sqlParams [][]*rdsdataservice.SqlParameter
	if parameters != nil && len(parameters) != 0 {
		sqlParams = make([][]*rdsdataservice.SqlParameter, 0, len(parameters))

		for _, rowParameters := range parameters {
			rowParams := make([]*rdsdataservice.SqlParameter, 0, len(parameters))
			for key, value := range rowParameters {
				field, err := valueToRdsField(value)
				if err != nil {
					context.AddContext("current_parameter", []interface{}{key, value})
					return nil, context.Wrap(err, "error converting parameter to Rds field")
				}
				param := &rdsdataservice.SqlParameter{
					Name: aws.String(key),
					Value: field,
				}
				rowParams = append(rowParams, param)
			}

			sqlParams = append(sqlParams, rowParams)
		}
	}

	executeStatementInput := rdsdataservice.BatchExecuteStatementInput{
		Database: aws.String(connexion.Database),
		ResourceArn: aws.String(connexion.ResourceArn),
		SecretArn: aws.String(connexion.SecretArn),
		Sql: aws.String(query),
		//Parameters: sqlParams,
		ParameterSets: sqlParams,
	}

	if transactionId != nil {
		executeStatementInput.TransactionId = transactionId
	}

	res, err := rdsClient.BatchExecuteStatement(&executeStatementInput)

	if err != nil {
		if strings.Contains(err.Error(), "Communications link failure") {
			return nil, context.Wrap(err, "RDS database is still waking up, try again in a minute")
		} else {
			return nil, context.Wrap(err, "error executing query")
		}
	}

	return res, nil
}

func ParseResults(fields []string, values [][]*rdsdataservice.Field)([]QueryResult, error){
	context := ctxerror.SetContext(map[string]interface{}{
		"fields": fields,
		"values": values,
	})

	results := make([]QueryResult, len(values))

	for i, val := range values {

		if len(val) != len(fields) {
			return nil, context.New("values and fields need to have the same length")
		}

		var result = make(QueryResult)
		for j, fieldValue := range val{
			parsedVal, err := rdsFieldToValue(fieldValue)
			if err != nil {
				return nil, context.Wrap(err, "unable to parse value")
			}

			result[fields[j]] = parsedVal
		}

		results[i] = result
	}

	return results, nil
}

func BeginTransaction(connexion AuroraConnexion) (string, error) {
	context := ctxerror.SetContext(map[string]interface{}{
		"connexion": connexion,
	})

	rdsClient := rdsdataservice.New(awsSession)

	output, err := rdsClient.BeginTransaction(&rdsdataservice.BeginTransactionInput{
		Database: aws.String(connexion.Database),
		ResourceArn: aws.String(connexion.ResourceArn),
		SecretArn: aws.String(connexion.SecretArn),
	})

	if err != nil {
		if strings.Contains(err.Error(), "Communications link failure") {
			return "", context.Wrap(err, "RDS database is still waking up, try again in a minute")
		} else {
			return "", context.Wrap(err, "error beginning transaction")
		}
	}

	return *output.TransactionId, nil
}

func CommitTransaction(connexion AuroraConnexion, transactionId string) error {
	context := ctxerror.SetContext(map[string]interface{}{
		"transactionId": transactionId,
		"connexion": connexion,
	})

	rdsClient := rdsdataservice.New(awsSession)

	_, err := rdsClient.CommitTransaction(&rdsdataservice.CommitTransactionInput{
		ResourceArn: aws.String(connexion.ResourceArn),
		SecretArn: aws.String(connexion.SecretArn),
		TransactionId: &transactionId,
	})
	if err != nil {
		if strings.Contains(err.Error(), "Communications link failure") {
			return context.Wrap(err, "RDS database is still waking up, try again in a minute")
		} else {
			return context.Wrap(err, "error commiting transaction")
		}
	}

	return nil
}

func RollbackTransaction(connexion AuroraConnexion, transactionId string) error {
	context := ctxerror.SetContext(map[string]interface{}{
		"transactionId": transactionId,
		"connexion": connexion,
	})


	rdsClient := rdsdataservice.New(awsSession)

	_, err := rdsClient.RollbackTransaction(&rdsdataservice.RollbackTransactionInput{
		ResourceArn: aws.String(connexion.ResourceArn),
		SecretArn: aws.String(connexion.SecretArn),
		TransactionId: &transactionId,
	})
	if err != nil {
		if strings.Contains(err.Error(), "Communications link failure") {
			return context.Wrap(err, "RDS database is still waking up, try again in a minute")
		} else {
			return context.Wrap(err, "error rolling back transaction")
		}
	}

	return nil
}

func valueToRdsField(value interface{}) (*rdsdataservice.Field, error) {
	context := ctxerror.SetContext(map[string]interface{}{
		"value": value,
	})

	if value == nil {
		return &rdsdataservice.Field{
			IsNull: aws.Bool(true),
		}, nil
	}

	if reflect.TypeOf(value).Kind() == reflect.Ptr {
		//its a pointer, and it cant be nil cause we checked above
		rValue := reflect.ValueOf(value)
		if rValue.IsNil() {
			return &rdsdataservice.Field{
				IsNull: aws.Bool(true),
			}, nil
		}
		//dereference pointer
		value = rValue.Elem().Interface()
	}

	switch t := value.(type) {
	case []byte:
		return &rdsdataservice.Field{
			BlobValue: t,
		}, nil
	case bool:
		return &rdsdataservice.Field{
			BooleanValue: &t,
		}, nil
	case float64:
		return &rdsdataservice.Field{
			DoubleValue: &t,
		}, nil
	case string:
		return &rdsdataservice.Field{
			StringValue: &t,
		}, nil
	case int64:
		return &rdsdataservice.Field{
			LongValue: &t,
		}, nil
	default:
		return nil, context.New("unknown type: " + reflect.TypeOf(value).String() + ", supported types are: float64, bool, []byte, int64, nil, string")
	}
}

func rdsFieldToValue(field *rdsdataservice.Field) (interface{}, error) {
	context := ctxerror.SetContext(map[string]interface{}{
		"field": field,
	})

	if field.IsNull != nil {
		if *field.IsNull {
			return nil, nil
		}
	}

	switch {
	case field.ArrayValue != nil:
		return parseArrayValue(*field.ArrayValue)
	case field.BlobValue != nil:
		return field.BlobValue, nil
	case field.BooleanValue != nil:
		return *field.BooleanValue, nil
	case field.DoubleValue != nil:
		return *field.DoubleValue, nil
	case field.LongValue != nil:
		return *field.LongValue, nil
	case field.StringValue != nil:
		return *field.StringValue, nil
	default:
		return nil, context.New("unknow field type")
	}
}

func parseArrayValue(value rdsdataservice.ArrayValue) (interface{}, error) {
	context := ctxerror.SetContext(map[string]interface{}{
		"value": value,
	})

	switch  {
	case value.ArrayValues != nil:
		values := []interface{}{}
		for _, val := range value.ArrayValues{
			val, err := parseArrayValue(*val)
			if err != nil{
				return nil, context.Wrap(err, "error while decoding value")
			}
			values = append(values, val)

			return values, nil
		}
	case value.BooleanValues != nil:
		return value.BooleanValues, nil
	case value.DoubleValues != nil:
		return value.DoubleValues, nil
	case value.LongValues != nil:
		return value.LongValues, nil
	case value.StringValues != nil:
		return value.StringValues, nil
	}

	return nil, context.New("unknow field type")
}

