import pandas as pd

class calculate_ageing_water_oil_formation:
    def __init__(self, cursor, id_equipment):
        self.cursor = cursor
        self.id_equipment = id_equipment

    def calculate_ageing_water_oil_formation_exec(self):
        query = '''
                SELECT 
                    e.Descricao
                    , ie.Descricao
                    , Variavel.Codigo
                    , Variavel.Descricao
                    , crv.Valor
                    , CAST(crv.DataMedicao AS DATE) AS dataMedicao
                FROM EngineCalculo.CalculoResultadoVariavel AS crv
                INNER JOIN EngineCalculo.Variavel 
                    ON Variavel.Id = crv.VariavelId
                INNER JOIN EngineCalculo.CalculoResultado AS ExecucaoCalculoResultado 
                    ON ExecucaoCalculoResultado.JobId = crv.JobId
                INNER JOIN EngineCalculo.CalculoResultadoEquipamento 
                    ON CalculoResultadoEquipamento.CalculoId = ExecucaoCalculoResultado.CalculoId 
                    AND CalculoResultadoEquipamento.JobId = ExecucaoCalculoResultado.JobId
                INNER JOIN Treetech.ViewEngine_UltimoJob
                    ON ViewEngine_UltimoJob.EquipamentoId = CalculoResultadoEquipamento.EquipamentoId 
                    AND ViewEngine_UltimoJob.JobId = ExecucaoCalculoResultado.JobId

                INNER JOIN Equipamento AS e 
                    ON e.Id = CalculoResultadoEquipamento.EquipamentoId
                    
                INNER JOIN EquipamentoInstalacaoEletrica AS eie 
                    ON e.Id = eie.EquipamentoId

                INNER JOIN InstalacaoEletrica AS ie 
                    ON  ie.Id = eie.InstalacaoEletricaId

                WHERE 1 = 1
                    AND e.Id = ?
                    AND Variavel.Codigo IN (
                     'TFB1_ON_VALOR' -- Temperatura para formação de bolhas enrolamento 1
                     , 'TFB2_ON_VALOR' -- Temperatura para formação de bolhas enrolamento 2
                     , 'TFB3_ON_VALOR' -- Temperatura para formação de bolhas enrolamento 1
                     , 'TA_OFF' -- Teor de Água por ensaio offline
                     , 'H2OP1_ON_VALOR' -- VALOR DO TEOR DE H20 NO PAPEL ENROLAMENTO 1
                     , 'H2OP2_ON_VALOR' -- VALOR DO TEOR DE H20 NO PAPEL ENROLAMENTO 2
                     , 'H2OP3_ON_VALOR' -- VALOR DO TEOR DE H20 NO PAPEL ENROLAMENTO 3
                     , 'MONIT_ENV_ISOL1' -- Monitor do Envelhecimento da Isolação - Enrolamento 1
                     , 'MONIT_ENV_ISOL2' -- Monitor do Envelhecimento da Isolação - Enrolamento 2
                     , 'MONIT_ENV_ISOL3' -- Monitor do Envelhecimento da Isolação - Enrolamento 3
                     ,  'TEMP_E1_ON_VALOR'
                     ,  'TEMP_E2_ON_VALOR'
                     ,  'TEMP_E3_ON_VALOR'

                            )
                        AND crv.Valor != 0
                GROUP BY 
                    e.Descricao
                    , ie.Descricao
                    , Variavel.Codigo
                    , Variavel.Descricao
                    , crv.Valor
                    , CAST(crv.DataMedicao AS DATE)

                ORDER BY CAST(crv.DataMedicao AS DATE) DESC
                        , e.Descricao DESC
        '''

        def get_variavel(descricao_variavel):
            if 'Envelhecimento' in descricao_variavel:
                return 'Envelhecimento'
            elif 'Temperatura no Enrolamento' in descricao_variavel:
                return 'Temperatura no Enrolamento'
            elif 'Temperatura' in descricao_variavel:
                return 'Temperatura'
            else:
                return 'Água'

        def get_enrolment_number(descricao_variavel):
                if '1' in descricao_variavel:
                    return 1
                elif '2' in descricao_variavel:
                    return 2
                elif '3' in descricao_variavel:
                    return 3
                else:
                    return 0
            
        self.cursor.execute(query, self.id_equipment)
        result_sql = self.cursor.fetchall()
        
        columns = ['DescricaoEquipamento', 'DescricaoInstalacaoEletrica', 'CodigoVariavel', 'DescricaoVariavel', 'Valor', 'DataMedicao']
        data = [dict(zip(columns, row)) for row in result_sql]
        df = pd.DataFrame(data)

        df['Variavel'] = df['DescricaoVariavel'].apply(get_variavel)
        df['Enrolamento'] = df['DescricaoVariavel'].apply(get_enrolment_number)
        novo_dataframe = df[df['DescricaoVariavel'].str.contains('Valor da Temperatura no Enrolamento')]
        indices_para_drop = df[df['DescricaoVariavel'].str.contains('Valor da Temperatura no Enrolamento')].index
        df = df.drop(indices_para_drop)
        df = df.loc[df.groupby(['Variavel'])['Valor'].idxmax()]

        def adjust_df(df):
            if 'Temperatura' in df['Variavel'].values:
                valor_enrolamento_temperatura = df.loc[df['Variavel'] == 'Temperatura', 'Enrolamento'].values[0]
                df_enrolamento_2 = novo_dataframe.loc[novo_dataframe['Enrolamento'] == valor_enrolamento_temperatura]
                df_concat = pd.concat([df, df_enrolamento_2])
                return df_concat
            else:
                return df

        return adjust_df(df)
