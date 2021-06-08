class desan :
    
    def __init__(self) :
        self.company = "PT. Global Loyalty Indonesia"
        #self.filepath = "C:\\python\\gli-data-science\\temp\\"
        self.filepath = ""
        self.gcpproject = "gli-dw-alfagift"
        self.gcpbucket = "gli-data-science"
        
        self.gcphyperpath = "hyper/"
        self.lochyperpath = "C:\\python\\hyper\\"
        #self.lochyperpath = "D:\\GLI\\hyper\\"         # tableau 63
        self.loc7zippath = 'C:\\"Program Files"\\7-Zip\\7z.exe'
        #self.loc7zippath = 'C:\\7-Zip\\7z.exe'         # tableau 63
      
      
    def insLogProses(self, pTag, pNamaProses, pPeriod, pFilename, user) :
        from datetime import datetime
        self.createDate = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        sql = """insert into log_proses (
				lp_tag,
				lp_nama_proses,
				lp_period,
				lp_filename,
				lp_status_proses,
				lp_tgl_mulai,
				lp_create_date,
				lp_update_date,
				lp_update_user)
			values (
				'"""+ pTag +"""',
				'"""+ pNamaProses +"""',
				'"""+ pPeriod +"""',
				'"""+ pFilename +"""',
				'P',
				'"""+ self.createDate +"""',
				'"""+ self.createDate +"""',
				'"""+ self.createDate +"""',
				'"""+ user +"""') returning lp_id;"""
        return sql;
        
        
    def updLogProses(self, pId, pError, pNote, user) :
        from datetime import datetime
        self.updateDate = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        sql = """update log_proses set
				lp_status_proses = 'Y',
				lp_tgl_selesai = '"""+ self.updateDate +"""',
				lp_error = '"""+ pError +"""',
				lp_note = '"""+ pNote +"""',
				lp_update_date = '"""+ self.updateDate +"""',
				lp_update_user = '"""+ user +"""'
			where lp_id = '"""+ pId +"'";
        return sql;
        
        
    def kirim_email_gagal(self, preceiver, psubject, pbody) :
        import smtplib
        from os.path import basename
        from email.mime.text import MIMEText
        from email.mime.multipart import MIMEMultipart
        #from email.mime.application import MIMEApplication

        sender = 'noreply@ponta.co.id'
        #preceiver = 'denny.yuwono@gli.id'
        receiver = preceiver.replace(' ','').split(',')

        msg = MIMEMultipart()

        msg['Subject'] = psubject
        msg['From'] = sender
        msg['To'] = preceiver
        body = MIMEText("Dear Master,<br><br>"+ pbody +"<br><br>Sincerely Yours,<br><b>Robot</b>", 'html')
        msg.attach(body)

        user = 'dyuwono@ponta.co.id'
        password = 'i love u'

        server = smtplib.SMTP("mail.ponta.co.id:587")
        server.login(user, password)
        server.sendmail(sender, receiver, msg.as_string())
        
        
    def kirim_email_noreply(self, preceiver, psubject, pbody, pattachment="") :
        import smtplib
        from os.path import basename
        from email.mime.text import MIMEText
        from email.mime.multipart import MIMEMultipart
        from email.mime.application import MIMEApplication

        sender = 'noreply@ponta.co.id'
        #preceiver = 'denny.yuwono@gli.id'
        receiver = preceiver.replace(' ','').split(',')

        msg = MIMEMultipart()

        msg['Subject'] = psubject
        msg['From'] = sender
        msg['To'] = preceiver
        body = MIMEText(pbody, 'html')
        msg.attach(body)

        if pattachment != "" :
            filename = self.filepath + pattachment
            with open(filename, 'rb') as f:
                part = MIMEApplication(f.read(), Name=basename(filename))

            part['Content-Disposition'] = 'attachment; filename="{}"'.format(basename(filename))
            msg.attach(part)

        user = 'dyuwono@ponta.co.id'
        password = 'i love u'

        server = smtplib.SMTP('mail.ponta.co.id:587')
        server.login(user, password)
        server.sendmail(sender, receiver, msg.as_string())
        
        
    def xls_openfile(self, filename) :
        import xlsxwriter

        self.workbook = xlsxwriter.Workbook(self.filepath + filename)
        self.worksheet = self.workbook.add_worksheet()
        
    def xls_header(self, header1, header2="", header3="", header4="", header5="") :
        from datetime import datetime
        createDate = datetime.now().strftime("%d-%b-%Y %H:%M:%S")
        bold = self.workbook.add_format({'bold': True})
        
        self.worksheet.write(0, 0, self.company, bold)           # company
        self.worksheet.write(1, 0, header1, bold)                # header 1
        self.row = 2
        if header2 != "" :
            self.worksheet.write(self.row, 0, header2, bold)          # header 2
            self.row += 1
        if header3 != "" :
            self.worksheet.write(self.row, 0, header3, bold)          # header 3
            self.row += 1
        if header4 != "" :
            self.worksheet.write(self.row, 0, header4, bold)          # header 4
            self.row += 1
        if header5 != "" :
            self.worksheet.write(self.row, 0, header5, bold)          # header 5
            self.row += 1
        self.worksheet.write(self.row, 0, "Created Date: "+ createDate, bold)                 # date create
        self.row += 2
        
    def xls_header_kol(self, list) :
        formater = self.workbook.add_format({'text_wrap': True, 'bold': True, 'font_color':'#ffffff', 'bg_color':'#4387bf', 'border':1, 'align':'center'})
        for col, data in enumerate(list):
            self.worksheet.write(self.row, col, data, formater)            
        self.row += 1    
            
            
    def xls_content(self, list) :
        formater = self.workbook.add_format({'border':1})
        numformater = self.workbook.add_format({'num_format': '#,##0', 'border':1})
        for col, data in enumerate(list):
            if str(data).isnumeric():    
                self.worksheet.write(self.row, col, data, numformater)
            else:
                self.worksheet.write(self.row, col, data, formater)            
        self.row += 1
            
            
    def xls_closefile(self) :
        self.workbook.close()
            
            
    def xls_deletefile(self, filename) :
        import os
        os.unlink(self.filepath + filename)        
