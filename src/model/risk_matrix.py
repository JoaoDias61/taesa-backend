import pandas as pd


class risk_matrix:
    def __init__(self, cursor, descricao, familia):
        self.cursor = cursor
        self.descricao = descricao
        self.familia = familia

    def risk_matrix_exec(self):
        query = '''
        EXEC [Treetech].[SPEngine_CalculoResultado]
        '''
        self.cursor.execute(query)
        resultado_sql = self.cursor.fetchall()

        colunas = ['EquipamentoId', 'PJ', 'Subestacao', 'Familia', 'TUC', 'FaseEletrica', 'CodigoOperacional', 'EquipamentoDescricao', 'JobId', 
                   'BCH_OLEO_TERC', 'TESTE_VALOR_VARIVEL', 'I2_CRI_RAPFT_RAPCON', 'REC_MOL_BCH_AT', 'IE_TR_AMB', 'IE_TR', 'IE_TR_ATV_FQ', 'DEN_OLEO', 
                   'REC_MOL_MULTIGAS_H2', 'I2_CRI_AUTONOMIA', 'MON_H2OP', 'DIM_ESTRATEGICO', 'DADOS_OLEO', 'REC_MOL_RESF', 'REC_MOL', 'BCH_CAD_TERC', 
                   'BCH_VAZ_TERC', 'NUM_ATV', 'REC_MOL_BOLHAS', 'INC_OLEO_BOL', 'BCH_TMP_SEC', 'IE_TR_OUT', 'DADOS_BCH', 'DIM_REPUTACAO', 'MON_BCH_MT', 
                   'BCH_OLEO_SEC', 'IE_TR_ATV_H2', 'INC_CMT_OP', 'DEN_BCH', 'BCH_TD_PRIM', 'INC_OLEO', 'IE_TR_BCH_FUGA', 'DEN_ATV', 'BCH_CAD_SEC', 
                   'BCH_EV_PRIM', 'IE_TR_BCH_TMP', 'IE_TR_BCH_VAZ', 'CRI_RAPFT_RAPCON', 'INC_ATV_H2OO', 'IE_TR_RESF_CAD', 'BCH_CAP_TERC', 
                   'REC_MOL_H2OP', 'INC_CMT_TM', 'REC_MOL_UMID_OLEO', 'MON_BCH_BT', 'IE_TR_OUT_CAD', 'INC_BCH_TMP', 'BCH_TMP_TERC', 'BCH_VAZ_SEC', 
                   'BCH_CF_PRIM', 'BCH_TMP_REATOR', 'INC_CMT_FQ', 'IE_TR_ACES', 'IE_TR_BCH_CAD', 'DADOS_EQP', 'REC_MOL_TMP', 'BCH_CAP_SEC', 'BCH_EV_TERC',
                   'MON_UMID_OLTC', 'MON_TMP_OLTC', 'NUM_BCH', 'REC_MOL_DESG_OLTC', 'I2_GRAFICO', 'INC_ATV_FQ', 'CRI_RAPFT_RAPTAE', 'REC_MOL_ESPECIALISTA', 
                   'IE_TR_TQP', 'IE_TR_RESF', 'REC_MOL_TORQUE_OLTC', 'MON_MULTIGAS', 'DEN_CMT', 'I2_CRI_NE_SIN', 'REC_MOL_BCH_BT', 'IE_TR_CMT_OLEO', 'MON_UMID_OLEO', 
                   'IE_TR_CMT_CC', 'MON_RESF', 'MON_BCH_AT', 'BCH_TMP_PRIM', 'INC_BCH_TD', 'TESTE_VALOR', 'INC_CMT_CC', 'REC_MOL_ENV_ISOL', 'IE_TR_ATV_H2OO', 
                   'IE_GRAFICO', 'REC_MOL_TMP_OLTC', 'IE_TR_BCH_CAP', 'IE_TR_OLEO', 'IE_TR_TQP_CAD', 'INC_ATV_DGA', 'INC_BCH_FUGA', 'BCH_EV_SEC', 'INC_CMT', 
                   'IE_TR_CMT_FQ', 'IR_TR', 'IE_TR_AMB_CAD', 'IE_TR_BCH_TD', 'INC_ATV_H2', 'I2_CRI_RAPFT_RAPTAE', 'IE_TR_CMT_OP', 'INC_ATV_H2OP', 'I2_CRI_MANUTENCAO', 
                   'BCH_TD_REATOR', 'INC_BCH', 'IE_TR_CMT_CAD', 'BCH_VAZ_PRIM', 'MON_BOLHAS', 'BCH_VAZ_N', 'IE_TR_ATV', 'I2_TR', 'DADOS_ATV', 'IE_TR_ATV_TMP', 'IE_TR_OLEO_CAD',
                   'BCH_CAD_PRIM', 'INC_BCH_CAP', 'IE_TR_ATV_H2OP', 'IE_TR_CMT', 'DIM_CONFORMIDADE', 'I2_CRI_TEMPO_DESLOC', 'IE_TR_ATV_EV', 'REC_MOL_BCH_MT', 'IE_TR_ACES_EV', 
                   'I2_CRI_REPOSICAO', 'IE_TR_ATV_GP', 'I2_CRI_SEGURANCA', 'IE_TR_CMT_TM', 'DADOS_CMT', 'BCH_OLEO_N', 'BCH_CF_TERC', 'IE_TR_BCH_OLEO', 'BCH_OLEO_PRIM', 'IE_TR_OLEO_EV', 'IE_TR_OLEO_BOL', 'INC_EQP', 'INC_ATV', 'NUM_CMT', 'BCH_CAP_REATOR', 'I2_CRI_AMBIENTE', 'IE_TR_ATV_CAD', 'MON_TORQUE_OLTC', 'IE_TR_AMB_EV', 'IE_TR_TQP_EV', 'REC_MOL_IEMAX', 'CRITERIOS_REC', 'BCH_TD_TERC', 'INC_ATV_GP', 'NUM_OLEO', 'MON_MULTIGAS_H2', 'MON_BOLSA', 'FASE_EQP', 'DIM_OPERACIONAL', 'REC_MOL_MULTIGAS', 'IE_TR_CMT_EV', 'IE_TR_BCH_EV', 'BCH_CAP_PRIM', 'MON_ENV_ISOL', 'MON_DESG_OLTC', 'REC_MOL_BOLSA', 'NIVEL_RISCO', 'INC_CMT_OLEO', 'IE_TR_BCH', 'BCH_CF_SEC', 'IE_TR_RESF_EV', 'INC_ATV_TMP', 'IE_TR_ACES_CAD', 'MON_TMP', 'BCH_TD_SEC', 'IE_TR_ATV_DGA', 'DIM_FINANCEIRO', 'REC_MOL_UMID_OLTC']

        colunas_matrix = ['EquipamentoId', 'PJ', 'Subestacao', 'Familia', 'TUC', 'FaseEletrica',
                          'CodigoOperacional', 'EquipamentoDescricao', 'JobId', 'IE_TR', 'I2_TR', 'IR_TR']

        dados = [dict(zip(colunas, row)) for row in resultado_sql]
        df = pd.DataFrame(dados)[colunas_matrix]

        if self.familia and self.descricao:
            df = df[(df['Familia'] == self.familia) & (df['EquipamentoDescricao'] == self.descricao)]

        if self.familia:
            df = df[df['Familia'] == self.familia]

        if self.descricao:
            df = df[df['EquipamentoDescricao'] == self.descricao]
        

        return df