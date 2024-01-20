import tempfile
import os
from ftplib import FTP

from snowflake.snowpark.session import Session

def create_session_object():
    connection_paramters = {
        "account":"*********",
        "user":"rpegu",
        "password":"********",
        "role":"demo_role",
        "warehouse":"demo_wh",
        "database":"snf_exntac",
        "schema" :"api_data"
    }

    session = Session.builder.configs(connection_paramters).create()
    return session


def read_ftp_transfer(Ftp_address,stage_name,snf_session):
    ftp =FTP(Ftp_address)# get the ftp address
    #login to ftp 
    ftp.login()
    ftp.cwd('pub/databases/gwas/summary_statistics/GCST90000001-GCST90001000/GCST90000014/harmonised')
    filenames = ftp.nlst()
    with tempfile.TemporaryDirectory() as td:
        count = 0
        for filename in filenames:
            if filename.endswith('txt'): # can be any extension like tsv.gz
                tmp_file = os.path.join(td,filename)
                #print(filename)
                #transfering the file into snowflake stage
                with open(tmp_file,'wb') as fh:
                    ftp.retrbinary('RETR ' + filename, fh.write)
                    snf_session.file.put(tmp_file,stage_name)
                    count +=1
    msg = "Number of file transfer: " + str(count)
    return(msg)

if __name__ == "__main__":
    snf_session = create_session_object()
    return_message= read_ftp_transfer('ftp.ebi.ac.uk','@mystage',snf_session)
    print(return_message)


