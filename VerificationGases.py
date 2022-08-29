
import pandas as pd
import glob as gb
import numpy as np 
from dateutil.relativedelta import relativedelta


class Air:
	
	def __init__(self,Variable=None,Estaciones=None,Fechai=None, Fechaf=None,Freq=5):

		self.Fechai      = (dt.datetime.now()-relativedelta(months=1)).strftime('%Y-%m-')+'01 01:00' if (Fechaf == None) else Fechai
		self.Fechaf      = (pd.to_datetime(self.Fechai)+ relativedelta(months=1)-dt.timedelta(hours=1)).strftime('%Y-%m-%d %H:%M') if (Fechaf == None) else Fechaf
		##### Modificarlo si se cambia de pc  #####################################
		self.Est         = pd.read_csv('Estaciones.txt',index_col=0).sort_values(by='Latitud',ascending=False)	
		self.Dropbox_path='/Users/siata/Dropbox (SIATA)/SGC_Siata_Aire/01_Gestion_Operativa/03_Registros/HojasVida_FichasTecnicas/ESTACIONES DE AIRE/'
		###########################################################################

		self.folder={'BAR':'01_BARBOSA/','BEL':'02_BELLO/','CAL':'03_CALDAS/','COP':'04_COPACABANA/','GIR':'05_GIRARDOTA/','ITA':'06_ITAGUI/','SUR':'07_LA ESTRELLA/','EST':'07_LA ESTRELLA/','MED':'08_MEDELLIN/','CEN':'08_MEDELLIN/','SAB':'09_SABANETA/','ENV':'10_ENVIGADO/'}
		self.nombre_tipo={'ZERO/SPAN':'CS','MULTIPUNTO':'MP','PRECISIÓN':'PR','AJUST. PREC.':'AP','VER. CERO':'VC','CERO_SPAN PREC.':'CS_PR'}
		self.fecha_range =pd.date_range(self.Fechai,self.Fechaf,freq='M')


	def Read_Verification(self,*args,**kwargs):
		self.var		= kwargs.get('var','NOx')
		self.tipo		= kwargs.get('tipo','CS') #CS: ZERO/SPAN, 'PRE'
		
		self.C={'CS':{},'MP':{},'PR':{},'AP':{},'VC':{}}

		analizador= 'O3(A)' if self.var=='Ozono' else 'NOX(A)' if self.var=='NOx' else 'CO(A)' if self.var=='CO' else 'SO2(A)' if self.var=='SO2' else None
		self.estaciones=self.Est['Nombre'][self.Est[self.var]==1].values
		for tipofile in self.C.keys():
			for est in self.estaciones:
				if (self.var in ['NOx','SO2']) or (est!='MED-PJIC' and self.var=='CO') or (est!='BEL-USBV' and self.var=='Ozono'):
					self.C[tipofile][est]={1:{},2:{},3:{},7:{},9 if tipofile=='MP' else 9:{},10:{},11 if self.var=='NOx' else 10:{},11 if tipofile=='MP' else 12:{}}				
				else:
					self.C[tipofile][est]={1:{},2:{},3:{},7.1:{},7.2:{},9 if tipofile=='MP' else 10:{},11 if tipofile=='MP' else 12:{} }								
		for est in self.estaciones:
			for date in self.fecha_range:
				print (date)
				print (est)
				print (self.Dropbox_path+self.folder[est[:3]]+est+'/'+analizador+'/VERIFICACIONES/'+date.strftime('%Y/%m')+'*')
				self.foldermes=gb.glob(self.Dropbox_path+self.folder[est[:3]]+est+'/'+analizador+'/VERIFICACIONES/'+date.strftime('%Y/%m')+'*')
				if len(self.foldermes)>0:
					XLS=gb.glob(self.foldermes[0]+'/*.xlsx')
					print (XLS)

					for name in XLS:
						print (name)
						self.xl = pd.ExcelFile(name)
						self.DF = self.xl.parse(0)
						if np.logical_not(pd.isnull(self.DF[self.DF.columns[8]][1])):
							version=int(self.DF[self.DF.columns[8]][1][-1])
						elif np.logical_not(pd.isnull(self.DF[self.DF.columns[8]][0])):
							version=int(self.DF[self.DF.columns[8]][0][-1])
						elif np.logical_not(pd.isnull(self.DF[self.DF.columns[9]][1])):
							version=int(self.DF[self.DF.columns[9]][1][-1])
						elif np.logical_not(pd.isnull(self.DF[self.DF.columns[10]][1])):
							version=int(self.DF[self.DF.columns[10]][1][-1])
						self.version=version

						dondej=np.where(self.DF['Unnamed: 1'].str.find('%d. '%(4+1))==0)[0][0]	
									
						try:
							dondei4=np.where(self.DF['Unnamed: 1'].str.find('%d. '%4)==0)[0][0]
							if pd.isnull(self.DF[self.DF.columns[2]][dondei4+1]):
								tipofile=self.nombre_tipo[self.DF[self.DF.columns[3]][dondei4+1]]
							else:
								tipofile=self.nombre_tipo[self.DF[self.DF.columns[2]][dondei4+1]]
						except:
							dondei4=np.where(self.DF['Unnamed: 1'].str.find('Seleccionar')==0)[0][0]
							tipofile=self.nombre_tipo[self.DF[self.DF.columns[3]][dondei4]]
						numtablas= ([1,2,3,7]+([9,11] if tipofile=='MP' else [10,12] if self.var!='NOx' else [9,10,12])) if (self.var in ['NOx','SO2']) or (est!='MED-PJIC' and self.var=='CO') or (est!='BEL-USBV' and self.var=='Ozono') else ([1,2,3,7.1,7.2]+([9,11] if tipofile=='MP' else [10,12])) ## 9 NOx
						for i in numtablas:

							print ('Leo la tabla'+str(i)+' de '+tipofile)
							if i not in [7.1,7.2]:
								dondei=np.where(self.DF['Unnamed: 1'].str.find('%d. '%i)==0)[0]
								if i==11 and len(np.where(self.DF['Unnamed: 1'].str.find('11. OBS')==0)[0]>0):
									dondei=[]
							else:
								dondei=np.where(self.DF['Unnamed: 1'].str.find('%s '%i)==0)[0]
							if len(dondei)>0:
								dondei=dondei[0]
								#dondej= dondei4 if i==3 else np.where(self.DF['Unnamed: 1'].str.find('%d. '%(i+1))==0)[0][0]
								if i==11:
									dondej=68+np.where(self.DF['Unnamed: 1'][70:].str.find('Zero')==0)[0][0]
									dondej11= dondei4 if i==3 else np.where(self.DF['Unnamed: 1'].str.find('%d. '%(i+1))==0)[0][0]
								elif i==9 and self.var!='NOx':
									dondej=(dondei-2)+np.where(self.DF['Unnamed: 1'][dondei:].str.find('Zero')==0)[0][0]
									dondej9= np.where(self.DF['Unnamed: 1'].str.find('%d. '%(i+1 if i!=10 else i+3))==0)[0][0]
								
									
								elif i==7.1 and self.var!='NOx':
									dondej= dondei4 if i==3 else np.where(self.DF['Unnamed: 1'].str.find('7.2 ')==0)[0][0]
								else:
									dondej= dondei4 if i==3 else np.where(self.DF['Unnamed: 1'].str.find('%d. '%(i+1))==0)[0]
									if  i==10 and (len(dondej)==0):
											dondej= dondei4 if i==3 else np.where(self.DF['Unnamed: 1'].str.find('%d. '%(i+3))==0)[0]									
									if  i==10 and (len(dondej)==0):
											dondej= np.where(self.DF['Unnamed: 1'].str.find('Observaciones')==0)[0]
									dondej=dondei4 if i==3 else dondej[0]

								if i in [1,2,3]:
									L=self.xl.parse(0, skiprows=dondei+1,nrows=dondej-dondei-2,header=0,usecols=np.arange(11)).dropna(axis=1, how='all').dropna(axis=0, how='all')
								else: 
									L=self.xl.parse(0, skiprows=dondei+1,nrows=dondej-dondei-(4 if i==7.1 else 2),header=2 if i in [7,7.2] else 1,usecols=np.arange(2 if i in [7,7.2] or (i==9 and self.var=='NOx') else 1,11)).dropna(axis=1, how='all').dropna(axis=0, how='all')
								
								self.L=L.copy()	
								if i==1: 
									temporal=pd.DataFrame(L[L.columns[:2]].values).append(L[L.columns[2:4]].values.tolist(),ignore_index=True)
									temporal[0][7] = temporal[1][7]
									temporal[1][7] = str(L[L.columns[4:]].values[2][0])[:10]
									temporal = temporal.dropna()
									temporal=temporal.set_index(0)

								elif i==2:
									temporal=pd.DataFrame(L[L.columns[:2]].values).append(L[L.columns[2:4]].values.tolist(),ignore_index=True).append(L[L.columns[4:]].values.tolist(),ignore_index=True)
									temporal = temporal.dropna()
									temporal=temporal.rename({1:fecha})

									temporal=temporal.set_index(0)

								elif i==3:

									temporal=L[L.index==(2 if self.var=='NOx' else 1)][L.columns[:8 if version>=9 or self.var!='NOx' else 6]]
									if version>=9 or self.var!='NOx':
										temporal=pd.DataFrame(temporal[temporal.columns[:2]].values).append(temporal[temporal.columns[2:4]].values.tolist(),ignore_index=True).append(temporal[temporal.columns[4:6]].values.tolist(),ignore_index=True).append(temporal[temporal.columns[6:8]].values.tolist(),ignore_index=True)
									else:
										temporal=pd.DataFrame(temporal[temporal.columns[:2]].values).append(temporal[temporal.columns[2:4]].values.tolist(),ignore_index=True).append(temporal[temporal.columns[4:6]].values.tolist(),ignore_index=True)

									temporal2=L[L.index==(5 if self.var=='NOx' else 3)][L.columns[:8 if version>=9 or self.var!='NOx' else 6]]
									if version>=9 or self.var!='NOx':
										temporal2=pd.DataFrame(temporal2[temporal2.columns[:2]].values).append(temporal2[temporal2.columns[2:4]].values.tolist(),ignore_index=True).append(temporal2[temporal2.columns[4:6]].values.tolist(),ignore_index=True).append(temporal2[temporal2.columns[6:8]].values.tolist(),ignore_index=True)
									else:
										temporal2=pd.DataFrame(temporal2[temporal2.columns[:2]].values).append(temporal2[temporal2.columns[2:4]].values.tolist(),ignore_index=True).append(temporal2[temporal2.columns[4:6]].values.tolist(),ignore_index=True)
									if self.var!='Ozono':
										if version>=9 and self.var=='CO':
											temporal1= self.L[self.L.index==(5 if self.var=='NOx' else 5)][self.L.columns[:8 if self.version>=9 or self.var!='NOx' else 6]]
											temporal1= pd.DataFrame(temporal1[temporal1.columns[:2]].values).append(temporal1[temporal1.columns[2:4]].values.tolist(),ignore_index=True).append(temporal1[temporal1.columns[4:6]].values.tolist(),ignore_index=True).append(temporal1[temporal1.columns[6:8]].values.tolist(),ignore_index=True)
										elif version>=9 or self.var!='NOx':
											temporal1=pd.DataFrame(self.L[self.L.columns[8:10]].dropna().values)
										else:
											temporal1=pd.DataFrame(self.L[self.L.columns[6:8]].dropna().values)
										temporal1=temporal1.rename({1:fecha})
										self.temporal1=temporal1
										temporal1=temporal1.set_index(0)
								if i in [7.2,9,10]:#[7,7.2,9,10]
									L=(L[L.columns[:4]]) if self.var!='NOx' else (L[L.columns[:8]])

																	
								if i in [7.1,7.2,7,9,10,11,12]:
									L=L.set_index(L.columns[-1 if i==7.1 else 0])
									temporal=L.stack()
									temporal=pd.DataFrame({fecha:temporal})	
									
								if i==9 and self.var!='NOx':
									L=self.xl.parse(0, skiprows=dondej+1,nrows=dondej9-dondej-2,header=0,usecols=np.arange(1,11)).dropna(axis=1, how='all').dropna(axis=0, how='all')
									L=L.set_index(L.columns[0])
									temporal1=L.stack()
									temporal1=pd.DataFrame({fecha:temporal1})				
								if i ==11:
									L=self.xl.parse(0, skiprows=dondej+1,nrows=dondej11-dondej-2,header=0,usecols=np.arange(1,11)).dropna(axis=1, how='all').dropna(axis=0, how='all')
									L=L.set_index(L.columns[0])
									temporal1=L.stack()
									temporal1=pd.DataFrame({fecha:temporal1})	
								self.temporal=temporal
								if len(self.C[tipofile][est][i])==0:
									temporal[(temporal=='---')|(temporal=='+')]=np.nan
									if i==1:

										fecha= str(L[L.columns[4:]].values[2][0])[:10]
									if i in [1,2]:

										self.C[tipofile][est][i]=temporal.T
									if i ==3:
										
										temporal2[(temporal2=='---')|(temporal2=='+')]=np.nan
										temporal.index=temporal[0]
										temporal=temporal.drop(columns=[0])
										temporal=temporal.rename({1:fecha})
										temporal2.index=temporal2[0]
										temporal2=temporal2.drop(columns=[0])
										temporal2=temporal2.rename({1:fecha})
										self.C[tipofile][est][i]={}
										self.C[tipofile][est][i]['Calibrador D']=temporal.T
										self.C[tipofile][est][i]['Generador AC']=temporal2.T
										if self.var!='Ozono':
											temporal1[(temporal1=='---')|(temporal1=='+')]=np.nan
											self.C[tipofile][est][i]['Cilindro G']=temporal1.T
									if i == 11:
										self.C[tipofile][est][i]={}
										temporal1[(temporal1=='---')|(temporal1=='+')]=np.nan
										self.C[tipofile][est][i]['calibration']=temporal
										self.C[tipofile][est][i]['zero']=temporal1
									if (i == 9) and self.var!='NOx':
										self.C[tipofile][est][i]={}
										temporal1[(temporal1=='---')|(temporal1=='+')]=np.nan
										self.C[tipofile][est][i]['punto']=temporal
										self.C[tipofile][est][i]['zero']=temporal1
									if i in [7,7.1,7.2,10,12] or (i==9 and self.var=='NOx'):

										self.C[tipofile][est][i]=temporal
								else:
									temporal[(temporal=='---')|(temporal=='+')]=np.nan
									if i==1:
										fecha= str(L[L.columns[4:]].values[2][0])[:10]
									if i in [1,2]:
										self.C[tipofile][est][i]=self.C[tipofile][est][i].append(temporal.T)
									if i ==3:
										
										temporal2[(temporal2=='---')|(temporal2=='+')]=np.nan
										temporal.index=temporal[0]
										temporal=temporal.drop(columns=[0])
										temporal=temporal.rename({1:fecha})
										temporal2.index=temporal2[0]
										temporal2=temporal2.drop(columns=[0])
										temporal2=temporal2.rename({1:fecha})
										self.C[tipofile][est][i]['Calibrador D']=self.C[tipofile][est][i]['Calibrador D'].append(temporal.T)
										self.C[tipofile][est][i]['Generador AC']=self.C[tipofile][est][i]['Generador AC'].append(temporal2.T)
										if self.var!='Ozono':
											temporal1[(temporal1=='---')|(temporal1=='+')]=np.nan
											self.C[tipofile][est][i]['Cilindro G']=self.C[tipofile][est][i]['Cilindro G'].append(temporal1.T)
									if i==11:
										temporal1[(temporal1=='---')|(temporal1=='+')]=np.nan
										self.C[tipofile][est][i]['calibration']=pd.concat([self.C[tipofile][est][i]['calibration'],temporal],axis=1)
										self.C[tipofile][est][i]['zero']=pd.concat([self.C[tipofile][est][i]['zero'],temporal1],axis=1)
									if i==9 and self.var!='NOx':
										temporal1[(temporal1=='---')|(temporal1=='+')]=np.nan
										self.C[tipofile][est][i]['punto']=pd.concat([self.C[tipofile][est][i]['punto'],temporal],axis=1)
										self.C[tipofile][est][i]['zero']=pd.concat([self.C[tipofile][est][i]['zero'],temporal1],axis=1)
									if i in [7.1,7.2,7,10,12] or (i==9 and self.var=='NOx'):
										self.temporal=temporal
										print (i)
										self.C[tipofile][est][i]=pd.concat([self.C[tipofile][est][i],temporal],axis=1)


#### BEL-USBV 7.1, 7.2 Ozono
#### MED-PJIC 7.1, 7.2 CO
