#! /usr/bin/env python3
import argparse
import json
import logging
import logging.config
import os
import pickle
import sys
import time
from concurrent import futures

import ServerSideExtension_pb2 as SSE
import grpc
from bigml.api import BigML
from bigml.ensemble import Ensemble

_ONE_DAY_IN_SECONDS = 60 * 60 * 24


class ExtensionService(SSE.ConnectorServicer):
    """
    A simple SSE-plugin created for the HelloWorld example.
    """

    def __init__(self, funcdef_file):
        """
        Class initializer.
        :param funcdef_file: a function definition JSON file
        """
        self._function_definitions = funcdef_file
        if not os.path.exists('logs'):
            os.mkdir('logs')
        log_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logger.config')
        logging.config.fileConfig(log_file)
        logging.info('Logging enabled')

    @property
    def function_definitions(self):
        """
        :return: json file with function definitions
        """
        return self._function_definitions

    @property
    def functions(self):
        """
        :return: Mapping of function id and implementation
        """
        return {
            0: '_predict_nba'
        }

    @staticmethod
    def _get_function_id(context):
        """
        Retrieve function id from header.
        :param context: context
        :return: function id
        """
        metadata = dict(context.invocation_metadata())
        header = SSE.FunctionRequestHeader()
        header.ParseFromString(metadata['qlik-functionrequestheader-bin'])

        return header.functionId

    """
    Implementation of added functions.
    """

    @staticmethod
    def _predict_nba(request, context):
        """

        :param request: iterable sequence of bundled rows
        :return: string
        """
        # Disable caching by uncomment the following two lines
        #md = (('qlik-cache', 'no-store'),)
        #context.send_initial_metadata(md)

        params = []

        # Iterate over bundled rows to retrieve data
        for request_rows in request:
            # Iterate over rows
            for row in request_rows.rows:
                # Retrieve string value of parameter and append to the params variable
                # Length of param is 1 since one column is received, the [0] collects the first value in the list
                param = [d.strData for d in row.duals][0]
                print('param:', param)
                params.append(param)

        print('params:', params)

        # Possible selections to predict
        opt_selections = ['Kevin Durant',
                          'Allen Iverson',
                          'Carmelo Anthony',
                          'Isaiah Thomas',
                          'Cory Jefferson',
                          'Robbie Hummel',
                          'Wesley Johnson']

        # Check selections
        if len(params) == 1 and any([selection in params for selection in opt_selections]):
            selection = params[0].split(' ')  # list of first name and last name
            file = 'NBA_data/Demo_predictPPGPk_{}_{}'.format(selection[0], selection[1])

            with open(file, 'rb') as f:
                data = pickle.load(f)
            print('data:', data)
            correct_res = data['NBA PPG']
            del data['NBA PPG']

            try:
                # Use pre-trained ensemble
                ensemble_link = 'ensemble/5727212049c4a15ca1004b77'
                ensemble = Ensemble(ensemble_link, api=BigML(dev_mode=True, domain='bigml.io'))  # saves locally
            except:
                err = sys.exc_info()
                logging.error('Unexpected error: {}, {}, {}'.format(err[2].tb_frame.f_code.co_filename,
                                                                  err[2].tb_lineno, err[1]))

            # Predict data using the trained ensemble
            res = ensemble.predict(data, with_confidence=True)
            print('res:', res)

            result = 'Predicted number of PPG: {} <br> ' \
                     'Correct number of PPG: {} <br>' \
                     'Confidence: {}'.format(round(res[0], 1), correct_res, round(res[1], 1))
        else:
            result = 'Not possible to predict.'

        # Create an iterable of dual with the result
        duals = iter([SSE.Dual(strData=result)])

        # Yield the row data as bundled rows
        yield SSE.BundledRows(rows=[SSE.Row(duals=duals)])


    """
    Implementation of rpc functions.
    """

    def GetCapabilities(self, request, context):
        """
        Get capabilities.
        Note that either request or context is used in the implementation of this method, but still added as
        parameters. The reason is that gRPC always sends both when making a function call and therefore we must include
        them to avoid error messages regarding too many parameters provided from the client.
        :param request: the request, not used in this method.
        :param context: the context, not used in this method.
        :return: the capabilities.
        """
        logging.info('GetCapabilities')
        # Create an instance of the Capabilities grpc message
        # Set values for pluginIdentifier and pluginVersion
        capabilities = SSE.Capabilities(allowScript=False,
                                        pluginIdentifier='Python NBA',
                                        pluginVersion='v1.0.0')

        with open(self.function_definitions) as json_file:
            # Iterate over each function definition and add data to the capabilities grpc message
            for definition in json.load(json_file)['Functions']:
                function = capabilities.functions.add()
                function.name = definition['Name']
                function.functionId = definition['Id']
                function.functionType = definition['Type']
                function.returnType = definition['ReturnType']

                # Retrieve name and type of each parameter
                for param_name, param_type in sorted(definition['Params'].items()):
                    function.params.add(name=param_name, dataType=param_type)

                logging.info('Adding to capabilities: {}({})'.format(function.name,
                                                                     [p.name for p in function.params]))

        return capabilities

    def ExecuteFunction(self, request_iterator, context):
        """
        Execute function call.
        :param request_iterator: an iterable sequence of Row.
        :param context: the context.
        :return: an iterable sequence of Row.
        """
        # Retrieve function id
        func_id = self._get_function_id(context)

        # Call corresponding function
        logging.info('ExecuteFunction (functionId: {})'.format(func_id))

        return getattr(self, self.functions[func_id])(request_iterator, context)


    """
    Implementation of the Server connecting to gRPC.
    """

    def Serve(self, port, pem_dir, bigml_credentials):
        """
        Sets up the gRPC Server with insecure connection on port
        :param port: port to listen on.
        :param pem_dir: Directory including certificates
        :param bigml_credentials: list of two strings; username and the api key
        :return: None
        """
        # Set up credentials for BigML
        os.environ['BIGML_USERNAME'] = bigml_credentials[0]
        os.environ['BIGML_API_KEY'] = bigml_credentials[1]
        os.environ['BIGML_AUTH'] = 'username^=%BIGML_USERNAME%;api_key^=%BIGML_API_KEY%'

        # Create gRPC server
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        SSE.add_ConnectorServicer_to_server(self, server)

        if pem_dir:
            # Secure connection
            with open(os.path.join(pem_dir, 'sse_server_key.pem'), 'rb') as f:
                private_key = f.read()
            with open(os.path.join(pem_dir, 'sse_server_cert.pem'), 'rb') as f:
                cert_chain = f.read()
            with open(os.path.join(pem_dir, 'root_cert.pem'), 'rb') as f:
                root_cert = f.read()
            credentials = grpc.ssl_server_credentials([(private_key, cert_chain)], root_cert, True)
            server.add_secure_port('[::]:{}'.format(port), credentials)
            logging.info('*** Running server in secure mode on port: {} ***'.format(port))
        else:
            # Insecure connection
            server.add_insecure_port('[::]:{}'.format(port))
            logging.info('*** Running server in insecure mode on port: {} ***'.format(port))

        # Start gRPC server
        server.start()
        try:
            while True:
                time.sleep(_ONE_DAY_IN_SECONDS)
        except KeyboardInterrupt:
            server.stop(0)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--port', nargs='?', default='50052')
    parser.add_argument('--pem_dir', nargs='?')
    parser.add_argument('--definition-file', nargs='?', default='FuncDefs_NBA.json')
    parser.add_argument('--BIGML_USERNAME')
    parser.add_argument('--BIGML_API_KEY')
    args = parser.parse_args()

    # need to locate the file when script is called from outside it's location dir.
    def_file = os.path.join(os.path.dirname(os.path.realpath(sys.argv[0])), args.definition_file)

    calc = ExtensionService(def_file)
    calc.Serve(args.port, args.pem_dir, (args.BIGML_USERNAME, args.BIGML_API_KEY))
