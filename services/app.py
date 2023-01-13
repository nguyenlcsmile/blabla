import json
import json
import boto3
from datetime import datetime, timedelta
import io
from decimal import Decimal
import pandas as pd
import traceback


def filter_count_log_v2(json_data, data_old):
    try:
        result = {
            "phone_new_cust": data_old.get('phone_new_cust', []),
            "check_cust_list_phone": data_old.get('check_cust_list_phone', []),
            "check_cust_box": {
                'total_exist': 0,
                'total_new': 0,
                'success': 0,
                'fail': 0
            },
            "submit_ekyc_list_phone": data_old.get('submit_ekyc_list_phone', []),
            "submit_ekyc_box": {
                'total_exist': 0,
                'total_new': 0,
                'success': 0,
                'fail': 0
            },
            "check_kyc_status_list_phone": data_old.get('check_kyc_status_list_phone', []),
            "submit_kyc_status_box": {
                'total_exist': 0,
                'total_new': 0,
                'success': 0,
                'fail': 0
            },
            "video_statement_list_cifId": data_old.get('video_statement_list_cifId', []),
            "video_statement_list_phone": data_old.get('video_statement_list_phone', []),
            "video_statement_box": {
                'total_exist': 0,
                'total_new': 0,
                'success': 0,
                'fail': 0
            },
            "face_match_list_cifId": data_old.get('face_match_list_cifId', []),
            "face_match_list_phone": data_old.get('face_match_list_phone', []),
            "face_match_box": {
                'total_exist': 0,
                'total_new': 0,
                'success': 0,
                'fail': 0,
                'new_cust_fail': 0,
                'exist_cust_fail': 0
            },
            "get_contract_list_cifId": data_old.get('get_contract_list_cifId', []),
            "get_contract_list_phone": data_old.get('get_contract_list_phone', []),
            "get_contract_box": {
                'total_exist': 0,
                'total_new': 0,
                'success': 0,
                'fail': 0
            },
            "sign_contract_list_cifId": data_old.get('sign_contract_list_cifId', []),
            "sign_contract_list_phone": data_old.get('sign_contract_list_phone', []),
            "sign_contract_box": {
                'total_exist': 0,
                'total_new': 0,
                'success': 0,
                'fail': 0,
                'new_cust_fail': 0,
                'exist_cust_fail': 0
            },
            "pass_onboarding": len(data_old.get('issue_card_list_cifId', [])),
            "issue_card_list_cifId": data_old.get('issue_card_list_cifId', []),
            "issue_card_list_phone": data_old.get('issue_card_list_phone', []),
            "issue_card_func": {
                'total_exist': 0,
                'total_new': 0,
                'total': 0,
                'success': 0,
                'fail': 0
            },
            "create_signature_list_cifId": data_old.get('create_signature_list_cifId', []),
            "create_signature_func": {
                'total': 0,
                'success': 0,
                'fail': 0
            },
            "request_statement_list_cifId": data_old.get('request_statement_list_cifId', []),
            "request_statement_func": {
                'total': 0,
                'success': 0,
                'fail': 0
            },
            "cwdr_vpbank_list_accNo": data_old.get('cwdr_vpbank_list_accNo', []),
            "cash_withdrawal_func": {
                'total': 0,
                'success': 0,
                'fail': 0
            },
            "open_td_list_cifId": data_old.get('open_td_list_cifId', []),
            "open_td_func": {
                'total': 0,
                'success': 0,
                'fail': 0
            },
            "closure_td_list_accNo": data_old.get('closure_td_list_accNo', []),
            "closure_td_func": {
                'total': 0,
                'success': 0,
                'fail': 0
            },
            "transaction_napas_list_accNo": data_old.get('transaction_napas_list_accNo', []),
            "transaction_napas": {
                'total': 0,
                'success': 0,
                'fail': 0
            },
            "systemRobo": 0,
            "systemUbit": 0,
            "systemFinacle": 0,
            "systemBackBase": 0,
            "systemESB": 0,
            "systemPega": 0,
            "systemPegaLos": 0,
            "systemPegaCrm": 0,
            "systemNaPas": 0,
            "systemISale": 0,
            "systemVTiger": 0,
            "systemVymo": 0,
            "systemHyperVerge": 0,
            "systemTutuka": 0,
            "systemTaseco": 0,
            "systemFPT_EContract": 0,
            "systemSmsGateWay": 0,
            "systemVietQR": 0,
            "systemCleverTab": 0,
            "errorFFPT03": 0,
            "criticalErrors": []
        }

        # error code
        errorRobo = ["FROBO01", "FROBO02", "FROBO03"]
        errorUbit = ["NF01", "AH01", "WS01", "EGS01"]
        errorFinacle = ["FFIN01", "FINR00", "FINA00", "FINT00", "FINC00", "FINC01",
                        "FINC02", "FINC03", "FIND00", "FINL00", "FINF00", "FINA01", "FINA02", "FINR01"]
        errorBackBase = ["FBB01"]
        errorESB = ["FESB01", "FESB02", "FESB03",
                    "FESB04", "FESB05", "FESB06", "ESBTO1"]
        errorPega = ["FPEGA01", "FPEGA02", "FPEGA03", "FPEGA04",
                     "FPEGA05", "FPEGA06", "FPEGA07", "FPEGA08"]
        errorPeGaLos = []
        errorPegaCrm = ["PGCU00"]
        errorNaPas = ["NPST00", "NPSR00", "NPSQ00"]
        errorISale = ["ISMU00"]
        errorVTiger = ["VTGU00"]
        errorVymo = []
        errorHyperVerge = []
        errorTutuka = []
        errorTaseco = []
        errorFPT_E_Contract = ["FFPT01", "FFPT02", "FFPT04"]
        errorSmsGateWay = ["SE01", "SE02", "MF01", "IV01"]
        errorVietQR = []
        errorCleverTab = []
        errorFFPT03 = ['FFPT03']

        for data in json_data:
            _source = data.get('_source', {})
            if _source:
                item = _source
            else:
                item = data
            event_code = item.get('eventCode', '')
            url_name = item.get('dataDetail', {}).get('url', '')
            status_code = item.get('dataDetail', {}).get('statusCode', 0)
            error_code = item.get('dataDetail', {}).get('errorCode', '')
            cif_id = item.get('cifId', '')
            phoneNo = item.get('phone', '')
            accountNo = item.get('accountNumber', '')

            # check phone box & onboarding customer
            if url_name == '/v1/jao/check-cust/GET':
                if phoneNo and phoneNo not in result['check_cust_list_phone']:
                    result['check_cust_list_phone'].append(phoneNo)
                    if status_code == 400 and error_code == 'NE01' and phoneNo not in result['phone_new_cust']:
                        result['phone_new_cust'].append(phoneNo)
                        result['check_cust_box']['total_new'] += 1
                    elif status_code == 200 and phoneNo not in result['phone_new_cust']:
                        result['check_cust_box']['total_exist'] += 1
                    elif status_code != 400 and status_code != 200:
                        result['check_cust_list_phone'].remove(phoneNo)
                if phoneNo and status_code == 200:
                    result['check_cust_box']['success'] += 1
                else:
                    result['check_cust_box']['fail'] += 1

            # Submit EKYC
            if url_name == '/v1/kyc/submit/POST':
                if phoneNo and phoneNo not in result['submit_ekyc_list_phone']:
                    result['submit_ekyc_list_phone'].append(phoneNo)
                    if status_code == 200 and phoneNo in result['phone_new_cust']:
                        result['submit_ekyc_box']['total_new'] += 1
                    elif status_code == 200 and phoneNo not in result['phone_new_cust']:
                        result['submit_ekyc_box']['total_exist'] += 1
                    elif status_code != 200:
                        result['submit_ekyc_list_phone'].remove(phoneNo)
                if phoneNo and status_code == 200:
                    result['submit_ekyc_box']['success'] += 1
                else:
                    result['submit_ekyc_box']['fail'] += 1

            # Status EKYC
            if url_name == '/v1/top-up/kyc-status/POST':
                if phoneNo and phoneNo not in result['check_kyc_status_list_phone']:
                    result['check_kyc_status_list_phone'].append(phoneNo)
                    if status_code == 200 and phoneNo in result['phone_new_cust']:
                        result['submit_kyc_status_box']['total_new'] += 1
                    elif status_code == 200 and phoneNo not in result['phone_new_cust']:
                        result['submit_kyc_status_box']['total_exist'] += 1
                    elif status_code != 200:
                        result['check_kyc_status_list_phone'].remove(phoneNo)
                if phoneNo and status_code == 200:
                    result['submit_kyc_status_box']['success'] += 1
                else:
                    result['submit_kyc_status_box']['fail'] += 1

            # Video Statement
            if url_name == '/v1/producer/push/PUT':
                producer_id = item.get('dataDetail').get('producerId')
                if producer_id == 'pusher-video-statement':
                    if cif_id and cif_id not in result['video_statement_list_cifId']:
                        result['video_statement_list_cifId'].append(cif_id)
                        if phoneNo:
                            result['video_statement_list_phone'].append(phoneNo)
                        if status_code == 200 and phoneNo in result['phone_new_cust']:
                            result['video_statement_box']['total_new'] += 1
                        elif status_code == 200 and phoneNo not in result['phone_new_cust']:
                            result['video_statement_box']['total_exist'] += 1
                        else:
                            result['video_statement_list_cifId'].remove(cif_id)
                    if cif_id and status_code == 200:
                        result['video_statement_box']['success'] += 1
                    else:
                        result['video_statement_box']['fail'] += 1

            # Face Match
            if url_name == '/v1/jao/check-face-match/POST':
                if cif_id and cif_id not in result['face_match_list_cifId']:
                    result['face_match_list_cifId'].append(cif_id)
                    if phoneNo:
                        result['face_match_list_phone'].append(phoneNo)
                    if phoneNo in result['phone_new_cust']:
                        if status_code == 200:
                            result['face_match_box']['total_new'] += 1
                        else:
                            if error_code == 'MR01':
                                result['face_match_box']['new_cust_fail'] += 1
                            if phoneNo:
                                result['face_match_list_phone'].remove(phoneNo)
                            result['face_match_list_cifId'].remove(cif_id)
                    else:
                        if status_code == 200:
                            result['face_match_box']['total_exist'] += 1
                        else:
                            if error_code == 'MR01':
                                result['face_match_box']['exist_cust_fail'] += 1
                            result['face_match_list_cifId'].remove(cif_id)
                if cif_id and status_code == 200:
                    result['face_match_box']['success'] += 1
                else:
                    result['face_match_box']['fail'] += 1

            # Get Contract
            if url_name == '/v1/jao/jao-contract/POST':
                if cif_id and cif_id not in result['get_contract_list_cifId']:
                    result['get_contract_list_cifId'].append(cif_id)
                    if phoneNo:
                        result['get_contract_list_phone'].append(phoneNo)
                    if status_code == 200:
                        result['get_contract_box']['total_exist'] += 1
                    else:
                        result['get_contract_list_cifId'].remove(cif_id)
                if cif_id and status_code == 200:
                    result['get_contract_box']['success'] += 1
                else:
                    result['get_contract_box']['fail'] += 1

            # Sign Contract Fail / E- Contract Functionals
            if url_name == '/v1/jao/verify-otp/POST':
                if cif_id and cif_id not in result['sign_contract_list_cifId']:
                    result['sign_contract_list_cifId'].append(cif_id)
                    if phoneNo:
                        result['sign_contract_list_phone'].append(phoneNo)
                    if phoneNo in result['phone_new_cust']:
                        if status_code == 200:
                            result['sign_contract_box']['total_new'] += 1
                        else:
                            if error_code == 'MR01':
                                result['sign_contract_box']['new_cust_fail'] += 1
                            result['sign_contract_list_cifId'].remove(cif_id)
                    else:
                        if status_code == 200:
                            result['sign_contract_box']['total_exist'] += 1
                        else:
                            if error_code == 'MR01':
                                result['sign_contract_box']['exist_cust_fail'] += 1
                            result['sign_contract_list_cifId'].remove(cif_id)
                if cif_id and status_code == 200:
                    result['sign_contract_box']['success'] += 1
                else:
                    result['sign_contract_box']['fail'] += 1

            # Functional Issue Card / Pass Onboarding
            if url_name == '/v1/service/issue-card/POST':
                if cif_id and cif_id not in result['issue_card_list_cifId']:
                    result['issue_card_func']['total'] += 1
                    result['issue_card_list_cifId'].append(cif_id)
                    if phoneNo:
                        result['issue_card_list_phone'].append(phoneNo)
                    if phoneNo in result['phone_new_cust']:
                        result['issue_card_func']['total_new'] += 1
                    else:
                        result['issue_card_func']['total_exist'] += 1
                if cif_id and status_code == 200:
                    result['issue_card_func']['success'] += 1
                else:
                    result['issue_card_func']['fail'] += 1

            # Functional Create Signature
            if url_name == '/v1/services/create-signature/POST':
                if cif_id and cif_id not in result['create_signature_list_cifId']:
                    result['create_signature_list_cifId'].append(cif_id)
                    result['create_signature_func']['total'] += 1
                if cif_id and status_code == 200:
                    result['create_signature_func']['success'] += 1
                else:
                    result['create_signature_func']['fail'] += 1

            # Functional Request Statement
            if url_name == '/v1/partners/account/export/statement/POST':
                if cif_id and cif_id not in result['request_statement_list_cifId']:
                    result['request_statement_list_cifId'].append(cif_id)
                    result['request_statement_func']['total'] += 1
                if cif_id and status_code == 200:
                    result['request_statement_func']['success'] += 1
                else:
                    result['request_statement_func']['fail'] += 1

            # Functional Cash Withdrawal
            if url_name == '/api/v1/cwdr/vpbank/txn/POST':
                if accountNo and accountNo not in result['cwdr_vpbank_list_accNo']:
                    result['cwdr_vpbank_list_accNo'].append(accountNo)
                    result['cash_withdrawal_func']['total'] += 1
                if accountNo and status_code == 200:
                    result['cash_withdrawal_func']['success'] += 1
                else:
                    result['cash_withdrawal_func']['fail'] += 1

            # Functional Open TD account
            if url_name == '/api/v1/fin/deposit/POST':
                if cif_id and cif_id not in result['open_td_list_cifId']:
                    result['open_td_list_cifId'].append(cif_id)
                    result['open_td_func']['total'] += 1
                if cif_id and status_code == 200:
                    result['open_td_func']['success'] += 1
                else:
                    result['open_td_func']['fail'] += 1

            # Functional Closure TD account
            if url_name == '/api/v1/fin/deposit/{accountId}/closure/POST':
                if accountNo and accountNo not in result['closure_td_list_accNo']:
                    result['closure_td_list_accNo'].append(accountNo)
                    result['closure_td_func']['total'] += 1
                if accountNo and status_code == 200:
                    result['closure_td_func']['success'] += 1
                else:
                    result['closure_td_func']['fail'] += 1

            # Transaction NAPAS accNo
            if url_name == '/v1/napas/IPN/POST':
                tran_status = item.get('dataDetail', {}).get('tranStatus', '')
                if accountNo and accountNo not in result['transaction_napas_list_accNo']:
                    result['transaction_napas_list_accNo'].append(accountNo)
                    result['transaction_napas']['total'] += 1

                if accountNo and tran_status == 'SUCCESS' and status_code == 200:
                    result['transaction_napas']['success'] += 1
                elif accountNo and tran_status == '' and status_code == 200:
                    result['transaction_napas']['success'] += 1
                else:
                    result['transaction_napas']['fail'] += 1

            if status_code == 500 and event_code:
                result['criticalErrors'].append(event_code)

            elif error_code in errorRobo and status_code != 200:
                result["systemRobo"] += 1

            elif error_code in errorUbit and status_code != 200:
                result["systemUbit"] += 1

            elif error_code in errorFinacle and status_code != 200:
                result["systemFinacle"] += 1

            elif error_code in errorBackBase and status_code != 200:
                result["systemBackBase"] += 1

            elif error_code in errorESB and status_code != 200:
                result["systemESB"] += 1

            elif error_code in errorPega and status_code != 200:
                result["systemPega"] += 1

            elif error_code in errorPeGaLos and status_code != 200:
                result["systemPegaLos"] += 1

            elif error_code in errorPegaCrm and status_code != 200:
                result["systemPegaCrm"] += 1

            elif error_code in errorNaPas and status_code != 200:
                result["systemNaPas"] += 1

            elif error_code in errorISale and status_code != 200:
                result["systemISale"] += 1

            elif error_code in errorVTiger and status_code != 200:
                result["systemVTiger"] += 1

            elif error_code in errorVymo and status_code != 200:
                result["systemVymo"] += 1

            elif error_code in errorHyperVerge and status_code != 200:
                result["systemHyperVerge"] += 1

            elif error_code in errorTutuka and status_code != 200:
                result["systemTutuka"] += 1

            elif error_code in errorTaseco and status_code != 200:
                result["systemTaseco"] += 1

            elif error_code in errorFPT_E_Contract and status_code != 200:
                result["systemFPT_EContract"] += 1

            elif error_code in errorSmsGateWay and status_code != 200:
                result["systemSmsGateWay"] += 1

            elif error_code in errorVietQR and status_code != 200:
                result["systemVietQR"] += 1

            elif error_code in errorCleverTab and status_code != 200:
                result["systemCleverTab"] += 1

            elif error_code in errorFFPT03 and status_code != 200:
                result["errorFFPT03"] += 1

        return result
    except Exception as e:
        print('Exception', traceback.format_exc())
        return e
    
def _load_s3():
    # Load s3 client
    return boto3.client('s3')

def myconverter(o):
    if isinstance(o, datetime):
        return o.__str__()
    if isinstance(o, Decimal):
        return int(o)

def index(event, context):
    try:
        file_name = event.get('file_name', '')
        key_s3 = f"DATA/PROD/LOGS/{file_name}/MONITORLOG/RECORD_TODAY_1.json"
        key_s3 = f"RECORD_TODAY_1.json"
        print('Check key s3:', key_s3)
        s3 = _load_s3()
        # print('env:', env_id)
        # print('bucket:', bucket)

        obj_entry = s3.get_object(Bucket='uamss', Key=key_s3)
        print('obj_entry', obj_entry)
        data_raw = obj_entry['Body'].read().decode()
        print('data_raw', data_raw)
        csvStringIO = io.StringIO(data_raw)
        # print('Check csv String IO')
        print('Check csvStringIO:', csvStringIO)
        read_json = pd.read_json(csvStringIO, lines=True)
        # print('>>>Check read_json:', read_json)
        format_dict = read_json.fillna('').to_dict('records')
        print('>>>Check format_dict:', format_dict)
        data_s3 = json.loads(json.dumps(
            format_dict, default=myconverter))
        print('Check data_s3:', data_s3)
        # data_s3 = json.loads(data_raw)
        result = filter_count_log_v2(data_s3, {})
        # print('Check result:', result)
        # DO NOT STORE TO S3 WEEK & MONTH

        put_s3 = f"DATA/PROD/LOGS/{file_name}/MONITORLOG/MONITORLOG_TODAY.json"
        print('Check key put s3:', put_s3)
        print('type', type(result))
        print('json dumps', json.dumps(result))
        res_put = s3.put_object(
            Body=json.dumps(result),  # json dumps object
            Bucket='uamss',
            Key=put_s3
        )
        print('res_put', res_put)

    except Exception as e:
        print('Exception', e)
        print('Exception', traceback.format_exc())
        return e
    return True
