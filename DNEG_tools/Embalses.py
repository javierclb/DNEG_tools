class Embalses():
    def __init__(self):
       return

    def cota(self,nombre,Volumen):

        if nombre=='HID.Guaiquivilo' or nombre=='GUAIQUIVILO':
            a0 = 1112.76569214446
            a1 = 0.204122891451908
            a2 = -2.08202656471027E-04
            a3 = 1.0379630226835E-07
            cota = a0 + (a1 * Volumen) + (a2 * Volumen ** 2) + (a3 * Volumen ** 3)
            return cota

        if nombre=='HID.Cipreses' or nombre=='CIPRESES':
            if Volumen <= 0:
                Cot_CIPRESES = 1280
            else:
                a0 = 134744.88984
                a1 = -211.91025423
                a2 = 0.0833132678
                DVol = (a1 * a1 - 4 * a2 * (a0 - Volumen)) ** 0.5
                Cot_CIPRESES = (-a1 + DVol) / (2 * a2)
            return Cot_CIPRESES

        if nombre=='HID.Lleuques' or nombre=='LLEUQUES':
            a0 = 1009.24816916763
            a1 = 1.89721565360991
            a2 = -2.37937658241295E-02
            a3 = 1.17973077683804E-04
            Cot_LLEUQUES = a0 + (a1 * Volumen) + (a2 * Volumen **2) + (a3 * Volumen ** 3)
            return Cot_LLEUQUES

        if nombre=='HID.Canutillar' or nombre=='CANUTILLAR':
            contador=0
            if Volumen < 449.739:
                contador+=1
                Cot_CANUTILLAR = 0.022235119 * Volumen + 220  #
            if Volumen >= 449.739 and Volumen <= 913.211:
                contador += 1
                Cot_CANUTILLAR = 0.021576276 * Volumen + 220.29631
            if contador==0:
                Cot_CANUTILLAR = 0.019715117 * Volumen + 221.99594
            return Cot_CANUTILLAR

        if nombre=='HID.Rucatayo' or nombre=='RUCATAYO':
            a0 = 144
            a1 = 148
            Cot_RUCATAYO = (Volumen * 1000000 - 4816560) / 397552.5 + a0
            if Cot_RUCATAYO > a1:
                Cot_RUCATAYO = a1
            return Cot_RUCATAYO

        if nombre=='HID.Pilmaiquen' or nombre=='PILMAIQUEN':
            a0 = 102
            a1 = 103.7
            Cot_PILMAIQUEN = Volumen / 11700 / 14148 * 1000000 + a0
            if Cot_PILMAIQUEN > a1:
                Cot_PILMAIQUEN = a1
            elif Cot_PILMAIQUEN < a0:
                Cot_PILMAIQUEN = a0
            return Cot_PILMAIQUEN

        if nombre=='HID.Pangue' or nombre=='PANGUE':
            a0 = 493
            a1 = 0.293889548
            a2 = -0.001273403
            a3 = 0.00000656608
            Cot_PANGUE = a0 + a1 * Volumen + a2 * Volumen ** 2 + a3 * Volumen ** 3
            return Cot_PANGUE

        if nombre == 'HID.Pehuenche' or nombre=='PEHUENCHE':
            a0 = 12532.0161
            a1 = -42.383595
            a2 = 0.0358801
            DVol = (a1 * a1 - 4 * a2 * (a0 - Volumen)) ** 0.5
            Cot_PEHUENCHE = (-a1 + DVol) / (2 * a2)
            return Cot_PEHUENCHE

        if nombre == 'HID.Angostura' or nombre=='ANGOSTURA':
            if Volumen < 0.27:
                Cot_ANGOSTURA = -48.4192665413133 * Volumen ** 2 + 29.5296605308872 * Volumen + 265.545840411463
            elif Volumen < 2.07:
                Cot_ANGOSTURA = -1.55585853866169 * Volumen ** 2 + 7.54048794244096 * Volumen + 268.431509764293
            elif Volumen < 4.88:
                Cot_ANGOSTURA = -0.184330182119195 * Volumen ** 2 + 3.37699284229226 * Volumen + 271.791610100975
            elif Volumen < 9.55:
                Cot_ANGOSTURA = -0.112192998077952 * Volumen ** 2 + 2.8581581455128 * Volumen + 272.728365457533
            elif Volumen < 18.29:
                Cot_ANGOSTURA = -2.53694952744581E-02 * Volumen ** 2 + 1.37651004368551 * Volumen + 279.169734659089
            elif Volumen < 33.73:
                Cot_ANGOSTURA = -8.42735262859683E-03 * Volumen ** 2 + 0.810786824299806 * Volumen + 283.990615589921
            elif Volumen < 68.92:
                Cot_ANGOSTURA = -2.05377126600161E-03 * Volumen ** 2 + 0.431707705240237 * Volumen + 289.774750779038
            elif Volumen < 105.82:
                Cot_ANGOSTURA = -5.41145244634336E-04 * Volumen ** 2 + 0.256320173086788 * Volumen + 294.905402099063
            elif True:
                Cot_ANGOSTURA = -4.80482772640695E-04 * Volumen ** 2 + 0.253576434458394 * Volumen + 294.546288065969
            return Cot_ANGOSTURA

        if nombre == 'HID.Eltoro' or nombre=='ELTORO':
            Datos=dict()
            Datos[1] = 0
            Datos[2] = 48.28954
            Datos[3] = 97.47766
            Datos[4] = 147.26746
            Datos[5] = 197.35679
            Datos[6] = 248.04517
            Datos[7] = 299.43508
            Datos[8] = 351.82341
            Datos[9] = 405.0131
            Datos[10] = 459.20122
            Datos[11] = 514.48761
            Datos[12] = 570.97736
            Datos[13] = 628.56274
            Datos[14] = 687.35149
            Datos[15] = 747.53804
            Datos[16] = 808.92531
            Datos[17] = 871.61054
            Datos[18] = 935.59635
            Datos[19] = 1000.88273
            Datos[20] = 1067.4697
            Datos[21] = 1135.25477
            Datos[22] = 1204.23795
            Datos[23] = 1274.32464
            Datos[24] = 1345.60945
            Datos[25] = 1417.89268
            Datos[26] = 1491.07712
            Datos[27] = 1565.25999
            Datos[28] = 1640.4439
            Datos[29] = 1716.42655
            Datos[30] = 1793.41026
            Datos[31] = 1871.19533
            Datos[32] = 1949.97619
            Datos[33] = 2029.56105
            Datos[34] = 2110.14171
            Datos[35] = 2191.52373
            Datos[36] = 2273.9068
            Datos[37] = 2357.18845
            Datos[38] = 2441.47116
            Datos[39] = 2526.65244
            Datos[40] = 2612.83215
            Datos[41] = 2700.01554
            Datos[42] = 2788.19472
            Datos[43] = 2877.37496
            Datos[44] = 2967.75593
            Datos[45] = 3059.03548
            Datos[46] = 3151.31608
            Datos[47] = 3244.69758
            Datos[48] = 3339.17471
            Datos[49] = 3434.65552
            Datos[50] = 3531.13476
            Datos[51] = 3628.71226
            Datos[52] = 3727.4905
            Datos[53] = 3827.36962
            Datos[54] = 3928.44686
            Datos[55] = 4030.62499
            Datos[56] = 4133.90402
            Datos[57] = 4238.58084
            Datos[58] = 4344.35855
            Datos[59] = 4451.33437
            Datos[60] = 4559.61076
            Datos[61] = 4668.98805
            Datos[62] = 4779.66329
            Datos[63] = 4891.53927
            Datos[64] = 5004.41367
            Datos[65] = 5118.38896
            Datos[66] = 5233.46515
            Datos[67] = 5349.83928
            Datos[68] = 5467.31431
            Datos[69] = 5585.88761
            Datos[70] = 5705.66164
            Datos[71] = 5826.53656

            (i,dc)=self.punteroA(nombre,71,Volumen,Datos)
            Cot_ELTORO = 1300   + i - 2 + dc
            if Cot_ELTORO < 1300:
                Cot_ELTORO = 1300
            elif    Cot_ELTORO > 1370:
                Cot_ELTORO = 1370
            return Cot_ELTORO

        if nombre == 'HID.Lmaule' or nombre=='LMAULE':
            Error_Cota = 0.005
            Num_de_Iter = 10
            Iter = 0
            CotIni = self.dcot(nombre,Volumen)

            while True:
                Iter = Iter + 1
                CotFin = CotIni - (self.volumen(nombre,CotIni) - Volumen) / self.dvol(nombre,CotIni)
                CondERR = abs(CotFin - CotIni) < Error_Cota
                CondITE = Iter > Num_de_Iter
                CotIni = CotFin

                if CondITE or CondERR:
                    break
            Cot_LMAULE = CotFin
            return Cot_LMAULE

        if nombre == 'HID.Machicura' or nombre=='MACHICURA':
            Error_Cota = 0.005
            Num_de_Iter = 10
            Iter = 0
            CotIni = self.dcot(nombre,Volumen)
            while True:
                Iter = Iter + 1
                CotFin = CotIni - (self.volumen(nombre,CotIni) - Volumen) / self.dvol(nombre,CotIni)
                CondERR = abs(CotFin - CotIni) < Error_Cota
                CondITE = Iter > Num_de_Iter
                CotIni = CotFin
                if CondERR or CondITE:
                    break
            Cot_MACHICURA = CotFin
            return Cot_MACHICURA

        if nombre == 'HID.Polcura' or nombre=='POLCURA':
            Error_Cota = 0.005
            Num_de_Iter = 10
            Iter = 0
            CotIni = self.dcot(nombre,Volumen)
            while True:
                Iter = Iter + 1
                CotFin = CotIni - (self.volumen(nombre,CotIni) - Volumen) / self.dvol(nombre,CotIni)
                CondERR = abs(CotFin - CotIni) < Error_Cota
                CondITE = Iter > Num_de_Iter
                CotIni = CotFin
                if CondERR or CondITE:
                    break
            Cot_POLCURA = CotFin
            return Cot_POLCURA

        if nombre == 'HID.Rapel' or nombre=='RAPEL':
            Error_Cota = 0.005
            Num_de_Iter = 10
            Iter = 0
            CotIni = self.dcot(nombre,Volumen)
            while True:
                Iter = Iter + 1
                CotFin = CotIni - (self.volumen(nombre,CotIni) - Volumen) / self.dvol(nombre,CotIni)
                CondERR = abs(CotFin - CotIni) < Error_Cota
                CondITE = Iter > Num_de_Iter
                CotIni = CotFin
                if CondERR or CondITE:
                    break

            Cot_RAPEL = CotFin
            return Cot_RAPEL

        if nombre == 'HID.Ralco' or nombre=='RALCO':
            Vol_Inf=dict()

            Vol_Inf[1] = 0
            Vol_Inf[2] = 0.02132
            Vol_Inf[3] = 0.43767
            Vol_Inf[4] = 4.90172
            Vol_Inf[5] = 15.43637
            Vol_Inf[6] = 22.730575
            Vol_Inf[7] = 24.318677
            Vol_Inf[8] = 25.984148
            Vol_Inf[9] = 27.732006

            Cotas=dict()
            Cotas[1] = 598
            Cotas[2] = 600
            Cotas[3] = 610
            Cotas[4] = 620
            Cotas[5] = 630
            Cotas[6] = 635
            Cotas[7] = 636
            Cotas[8] = 637
            Cotas[9] = 638
            LimIter = 100
            Error = 0.0001

            if Volumen <= Vol_Inf[9]:
                i= self.punteroA(nombre, 9, Volumen, Vol_Inf)
                Cot_Ralco = ((Cotas[i] - Cotas[i - 1]) / (Vol_Inf[i] - Vol_Inf[i - 1])) * (Volumen - Vol_Inf[i]) + Cotas[i]
            else:
                Cota_R = 708
                DCot = 9999
                Iter = 0
                while DCot > Error and Iter < LimIter:
                    Iter = Iter + 1
                    Vol_aux = self.volumen(nombre,Cota_R)
                    dVol_aux = self.dvol(nombre,Cota_R)
                    Cota_A = Cota_R + (Volumen - Vol_aux) / dVol_aux
                    DCot = abs(Cota_R - Cota_A)
                    Cota_R = Cota_A
                    Cot_Ralco = Cota_R
            return Cot_Ralco

        if nombre == 'HID.Colbun' or nombre=='COLBUN':
            Error_Cota = 0.001
            if Volumen < 319.1:
                Cot_COLBUN = 393
            elif Volumen < 380.22:
                Cotas=dict()
                Cotas[1] = 393
                Cotas[2] = 394
                Cotas[3] = 395
                Cotas[4] = 396
                Cotas[5] = 397
                volumenes=dict()
                volumenes[1] = 319.1
                volumenes[2] = 333.76
                volumenes[3] = 348.83
                volumenes[4] = 364.32
                volumenes[5] = 380.22
                (i,dc)= self.punteroA(nombre,5, Volumen, volumenes)
                Cot_COLBUN = Cotas[i - 1] + dc * (Cotas[i] - Cotas[i - 1])

            else:
                Num_de_Iter = 15
                Iter = 0
                CotIni = self.dcot(nombre,Volumen)
                flag=True
                while flag:
                    Iter = Iter + 1
                    CotFin = CotIni - (self.volumen(nombre,CotIni) - Volumen) / self.dvol(nombre,CotIni)
                    CondERR = abs(CotFin - CotIni) < Error_Cota
                    CondITE = Iter > Num_de_Iter
                    CotIni = CotFin
                    if CondITE or CondERR:
                        break
                Cot_COLBUN = CotFin
            return Cot_COLBUN

    def dcot(self,nombre,Volumen):

        if nombre == 'HID.Lmaule' or nombre=='LMAULE':
            a0 = 3.25854232403699E-03
            a1 = 0.025405983908303
            a2 = -1.35727677749965E-05
            a3 = 2.49264011608606E-08
            a4 = -3.23135007234829E-11
            a5 = 2.43385209187998E-14
            a6 = -9.6847814254483E-18
            a7 = 1.57390037611625E-21
            CotEST_LMAULE = a0 + (a1 * Volumen) + (a2 * Volumen ** 2) + (a3 * Volumen ** 3) + (a4 * Volumen ** 4) + (a5 * Volumen ** 5)
            CotEST_LMAULE = CotEST_LMAULE + (a6 * Volumen ** 6) + (a7 * Volumen ** 7)
            CotEST_LMAULE = CotEST_LMAULE + 2152.135
            CotEST_LMAULE = min(CotEST_LMAULE, 2180.3)
            return CotEST_LMAULE

        if nombre == 'HID.Machicura' or nombre=='MACHICURA':
            a0 = 253.9619
            a1 = 0.243919
            a2 = -0.006546
            a3 = 0.000503
            a4 = -0.000022
            a5 = 0.000000382368
            CotEST_MACHICURA = a0 + (a1 * Volumen) + (a2 * Volumen ** 2) + (a3 * Volumen ** 3) + (a4 * Volumen ** 4) + (a5 * Volumen ** 5)
            return CotEST_MACHICURA

        if nombre == 'HID.Polcura' or nombre=='POLCURA':
            a0 = 730.02173
            a1 = 8.94574
            a2 = -8.50159
            a3 = 13.99204
            a4 = -13.87739
            a5 = 5.10665
            DVol = Volumen
            CotEST_POLCURA = a0 + (a1 * DVol) + (a2 * DVol ** 2) + (a3 * DVol ** 3) + (a4 * DVol ** 4) + (a5 * DVol ** 5)
            return CotEST_POLCURA

        if nombre == 'HID.Rapel' or nombre=='RAPEL':
            a0 = 89.57534303
            a1 = 0.45374052
            a2 = 0.02412212
            a3 = -0.00068196
            CotEST_RAPEL = a0 + (a1 * Volumen ** 0.5) + (a2 * Volumen) + (a3 * Volumen ** 1.5)
            return CotEST_RAPEL

        if nombre == 'HID.Colbun' or nombre=='COLBUN':
            a0 = 364.83334314
            a1 = 0.1113822656
            a2 = -0.00008523
            a3 = 0.000000044
            a4 = -1.3231988E-11
            a5 = 1.8734156E-15
            CotEST_COLBUN = a0 + (a1 * Volumen) + (a2 * Volumen ** 2) + (a3 * Volumen ** 3) + (a4 * Volumen ** 4) + (a5 * Volumen ** 5)
            return CotEST_COLBUN

    def volumen(self,nombre,Cota):

        if nombre == 'HID.Guaiquivilo' or nombre=='GUAQUIVILO':
            a3 = 3.49271697417765E-04
            a2 = -1.11651634223517
            a1 = 1190.27813870034
            a0 = -423196.554648788
            Vmax = 713.18
            Vol = a0 + (a1 * Cota) + (a2 * Cota ** 2) + (a3 * Cota ** 3)
            if Vol>Vmax:
                Vol=Vmax
            return Vol

        if nombre == 'HID.Cipreses' or nombre=='CIPRESES':

            if Cota <= 1280:
                Vol_CIPRESES = 0  #
            else:
                a0 = 134744.88984
                a1 = -211.91025423
                a2 = 0.0833132678
                Vol_CIPRESES = a0 + (a1 * Cota) + (a2 * Cota ** 2)
            return Vol_CIPRESES

        if nombre == 'HID.Lleuques' or nombre=='LLEUQUES':
            a3 = 7.34648088052782E-05
            a2 = -0.209263122091889
            a1 = 198.09402889387
            a0 = -62295.8602449586
            Vmax = 53.33
            Vol_LLEUQUES = a0 + (a1 * Cota) + (a2 * Cota ** 2) + (a3 * Cota ** 3)
            if Vol_LLEUQUES > Vmax:
                Vol_LLEUQUES = Vmax
            return Vol_LLEUQUES

        if nombre == 'HID.Canutillar' or nombre=='CANUTILLAR':
            contador=0
            if Cota < 230:
                contador+=1
                Vol_CANUTILLAR = 44.9739 * Cota - 9894.258
            if Cota >= 230 and Cota <= 240:
                contador += 1
                Vol_CANUTILLAR = 46.3472 * Cota - 10210.117

            if contador ==0:
                Vol_CANUTILLAR = 50.7225 * Cota - 11260.189
            return Vol_CANUTILLAR

        if nombre == 'HID.Rucatayo' or nombre=='RUCATAYO':
            a0 = 144
            a1 = 148
            if Cota > a1:
                Cota = a1
            Vol_RUCATAYO = ((Cota - a0) * 397552.5 + 4816560) / 1000000
            return Vol_RUCATAYO

        if nombre == 'HID.Pilmaiquen' or nombre=='PILMAIQUEN':
            a0 = 102
            a1 = 103.7
            if Cota <= a1:
                Vol_PILMAIQUEN = (Cota - a0) * 117 * 100 * 14148 / 1000000
            else:
                Vol_PILMAIQUEN = (a1 - a0) * 117 * 100 * 14148 / 1000000
            return Vol_PILMAIQUEN

        if nombre=='HID.Pangue' or nombre=='PANGUE':
            a0 = 7091.6
            a1 = -32.43
            a2 = 0.0366

            if Cota < 493:
                Vol_PANGUE = 0
            else:
                Vol_PANGUE = a0 + a1 * Cota + a2 * Cota ** 2
            if Vol_PANGUE<0:
                Vol_PANGUE=0
            return Vol_PANGUE

        if nombre == 'HID.Pehuenche' or nombre=='PEHUENCHE':
            a0 = 12532.0161
            a1 = -42.383595
            a2 = 0.0358801
            Vol_PEHUENCHE = a0 + (a1 * Cota) + (a2 * Cota ** 2)
            return Vol_PEHUENCHE

        if nombre == 'HID.Angostura' or nombre=='ANGOSTURA':
            if Cota < 280:  #
                Vol_ANGOSTURA = 1.42084270641058E-02 * Cota ** 2 - 7.55794967587359 * Cota + 1005.0989369465
            elif Cota < 290:
                Vol_ANGOSTURA = 0.023812857004521 * Cota ** 2 - 12.9330770375678 * Cota + 1757.21857766058
            elif Cota <296:
                Vol_ANGOSTURA = 5.76911113283868E-02 * Cota ** 2 - 32.3727074657782 * Cota + 4545.81085866535
            elif Cota <302:
                Vol_ANGOSTURA = 0.107207786948566 * Cota ** 2 - 61.5307295123461 * Cota + 8838.26697394881
            elif Cota < 310:
                Vol_ANGOSTURA = 0.141958163277288 * Cota ** 2 - 82.4673316143121 * Cota + 11991.7130189995
            elif Cota < 316:
                Vol_ANGOSTURA = 0.112940353957764 * Cota ** 2 - 64.5547375536446 * Cota + 9227.31750858668
            elif True:
                Vol_ANGOSTURA = 0.172040377016484 * Cota ** 2 - 102.168372622437 * Cota + 15211.766064903
            return Vol_ANGOSTURA

        if nombre == 'HID.Eltoro' or nombre=='ELTORO':
            Datos=dict()
            Datos[1] = 0
            Datos[2] = 48.28954
            Datos[3] = 97.47766
            Datos[4] = 147.26746
            Datos[5] = 197.35679
            Datos[6] = 248.04517
            Datos[7] = 299.43508
            Datos[8] = 351.82341
            Datos[9] = 405.0131
            Datos[10] = 459.20122
            Datos[11] = 514.48761
            Datos[12] = 570.97736
            Datos[13] = 628.56274
            Datos[14] = 687.35149
            Datos[15] = 747.53804
            Datos[16] = 808.92531
            Datos[17] = 871.61054
            Datos[18] = 935.59635
            Datos[19] = 1000.88273
            Datos[20] = 1067.4697
            Datos[21] = 1135.25477
            Datos[22] = 1204.23795
            Datos[23] = 1274.32464
            Datos[24] = 1345.60945
            Datos[25] = 1417.89268
            Datos[26] = 1491.07712
            Datos[27] = 1565.25999
            Datos[28] = 1640.4439
            Datos[29] = 1716.42655
            Datos[30] = 1793.41026
            Datos[31] = 1871.19533
            Datos[32] = 1949.97619
            Datos[33] = 2029.56105
            Datos[34] = 2110.14171
            Datos[35] = 2191.52373
            Datos[36] = 2273.9068
            Datos[37] = 2357.18845
            Datos[38] = 2441.47116
            Datos[39] = 2526.65244
            Datos[40] = 2612.83215
            Datos[41] = 2700.01554
            Datos[42] = 2788.19472
            Datos[43] = 2877.37496
            Datos[44] = 2967.75593
            Datos[45] = 3059.03548
            Datos[46] = 3151.31608
            Datos[47] = 3244.69758
            Datos[48] = 3339.17471
            Datos[49] = 3434.65552
            Datos[50] = 3531.13476
            Datos[51] = 3628.71226
            Datos[52] = 3727.4905
            Datos[53] = 3827.36962
            Datos[54] = 3928.44686
            Datos[55] = 4030.62499
            Datos[56] = 4133.90402
            Datos[57] = 4238.58084
            Datos[58] = 4344.35855
            Datos[59] = 4451.33437
            Datos[60] = 4559.61076
            Datos[61] = 4668.98805
            Datos[62] = 4779.66329
            Datos[63] = 4891.53927
            Datos[64] = 5004.41367
            Datos[65] = 5118.38896
            Datos[66] = 5233.46515
            Datos[67] = 5349.83928
            Datos[68] = 5467.31431
            Datos[69] = 5585.88761
            Datos[70] = 5705.66164
            Datos[71] = 5826.53656

            if Cota >= 1370:
                Vol_ELTORO = 5826.53656
            else:
                Cota= Cota-1300
                i = min(max(int(Cota) + 2,2), 71)
                Vol_ELTORO = Datos[i - 1] + (Cota - int(Cota)) * (Datos[i] - Datos[i - 1])
                Vol_ELTORO = max(Vol_ELTORO, 0)
            return Vol_ELTORO

        if nombre == 'HID.Lmaule' or nombre=='LMAULE':
            DCota = Cota - 2152.135
            a0 = -0.426511610904754
            a1 = 39.85091749344
            a2 = 0.713891558517388
            a3 = -2.68621789452889E-02
            a4 = 7.69400535914122E-04
            a5 = -8.51368088853222E-06
            Vol_LMAULE = a0 + (a1 * DCota) + (a2 * DCota ** 2) + (a3 * DCota ** 3) + (a4 * DCota ** 4) + (a5 * DCota ** 5)
            Vol_LMAULE = max(Vol_LMAULE, 0)
            return Vol_LMAULE

        if nombre == 'HID.Machicura' or nombre=='MACHICURA':
            a0 = 0.220082
            a1 = 3.869693
            a2 = 0.854351
            a3 = -0.346473
            a4 = 0.080443
            a5 = -0.007131
            if Cota < 254.5:
                Vol_MACHICURA = 0
            else:
                DCota = Cota - 254
                Vol_MACHICURA = a0 + (a1 * DCota) + (a2 * DCota ** 2) + (a3 * DCota ** 3) + (a4 * DCota ** 4) + (a5 * DCota ** 5)
            return Vol_MACHICURA

        if nombre == 'HID.Polcura' or nombre=='POLCURA':
            a0 = 0.6976365827
            a1 = 90.859293303
            a2 = 40.08341237
            a3 = -13.9725593488
            a4 = 2.5864430194
            a5 = -0.159930272
            DCota = Cota - 730
            Vol_POLCURA = (a0 + (a1 * DCota) + (a2 * DCota ** 2) + (a3 * DCota ** 3) + (a4 * DCota ** 4) + (a5 * DCota ** 5)) / 1000
            return Vol_POLCURA

        if nombre == 'HID.Rapel' or nombre=='RAPEL':
            a0 = -36039.35
            a1 = 1279.686867
            a2 = -15.1802416
            a3 = 0.060121028
            Vol_RAPEL = a0 + (a1 * Cota) + (a2 * Cota ** 2) + (a3 * Cota ** 3)
            Vol_RAPEL = max(Vol_RAPEL, 65.3)
            return Vol_RAPEL

        if nombre == 'HID.Ralco' or nombre=='RALCO':
            Vol_Inf=dict()
            Vol_Inf[1] = 0
            Vol_Inf[2] = 0.02132
            Vol_Inf[3] = 0.43767
            Vol_Inf[4] = 4.90172
            Vol_Inf[5] = 15.43637
            Vol_Inf[6] = 22.730575
            Vol_Inf[7] = 24.318677
            Vol_Inf[8] = 25.984148
            Vol_Inf[9] = 27.732006
            Cotas=dict()
            Cotas[1] = 598
            Cotas[2] = 600
            Cotas[3] = 610
            Cotas[4] = 620
            Cotas[5] = 630
            Cotas[6] = 635
            Cotas[7] = 636
            Cotas[8] = 637
            Cotas[9] = 638

            a3 = 0.9869
            a2 = -72.676
            a1 = 2789.6
            a0 = -30351
            if Cota<= Cotas[9]:
                i=self.punteroA(nombre,9,Cota,Cotas)
                Vol_Ralco = ((Vol_Inf[i] - Vol_Inf[i - 1]) / (Cotas[i] - Cotas[i - 1])) * (Cota - Cotas[i]) + Vol_Inf[i]
            else:
                Cota_R = Cota - Cotas[1]
                Vol_Ralco = (a0 + a1 * Cota_R + a2 * Cota_R ** 2 + a3 * Cota_R ** 3) / 1000
            return Vol_Ralco

        if nombre == 'HID.Colbun' or nombre=='COLBUN':
            a3 = 215.679132
            a2 = -564.993651
            a1 = 496.907289
            a0 = -146.591083
            Cmax = 437
            Vmax = 1550.63
            if Cota < 393:
                Vol_COLBUN = 319.1
            elif Cota < 397:
                Cotas=dict()
                Cotas[1] = 393
                Cotas[2] = 394
                Cotas[3] = 395
                Cotas[4] = 396
                Cotas[5] = 397
                volumenes=dict()
                volumenes[1] = 319.1
                volumenes[2] = 333.76
                volumenes[3] = 348.83
                volumenes[4] = 364.32
                volumenes[5] = 380.22
                i = int(Cota - 392)
                m = volumenes[i + 1] - volumenes[i]
                b = volumenes[i] - Cotas[i] * m
                Vol_COLBUN = m * Cota + b

            else:
                Vol_COLBUN = (a1 * (Cota / Cmax) + a2 * (Cota / Cmax) ** 2 + a3 * (Cota / Cmax) ** 3 + a0) * Vmax
            return Vol_COLBUN

    def dvol(self,nombre,Cota):

        if nombre == 'HID.Lmaule' or nombre=='LMAULE':
            DCota = Cota - 2152.135
            a0 = -0.426511610904754
            a1 = 39.85091749344
            a2 = 0.713891558517388
            a3 = -2.68621789452889E-02
            a4 = 7.69400535914122E-04
            a5 = -8.51368088853222E-06
            dVol_LMAULE = (a1) + 2 * (a2 * DCota) + 3 * (a3 * DCota ** 2) + 4 * (a4 * DCota ** 3) + 5 * (a5 * DCota ** 4)
            return dVol_LMAULE

        if nombre == 'HID.Machicura' or nombre=='MACHICURA':
            a1 = 3.869693
            a2 = 0.854351
            a3 = -0.346473
            a4 = 0.080443
            a5 = -0.007131
            DCota = Cota - 254
            dVol_MACHICURA = (a1) + 2 * (a2 * DCota) + 3 * (a3 * DCota ** 2) + 4 * (a4 * DCota ** 3) + 5 * (a5 * DCota **4)
            return dVol_MACHICURA

        if nombre == 'HID.Polcura' or nombre=='POLCURA':
            a1 = 90.859293303
            a2 = 40.08341237
            a3 = -13.9725593488
            a4 = 2.5864430194
            a5 = -0.159930272
            DCota = Cota - 730
            dVol_POLCURA = ((a1) + 2 * (a2 * DCota) + 3 * (a3 * DCota ** 2) + 4 * (a4 * DCota ** 3) + 5 * (a5 * DCota **4)) / 1000
            return dVol_POLCURA

        if nombre == 'HID.Rapel' or nombre=='RAPEL':
            a1 = 1279.686867
            a2 = -15.1802416
            a3 = 0.060121028
            dVol_RAPEL = (a1) + 2 * (a2 * Cota) + 3 * (a3 * Cota ** 2)

            return dVol_RAPEL

        if nombre == 'HID.Ralco' or nombre=='RALCO':
            a3 = 0.9869
            a2 = -72.676
            a1 = 2789.6
            Cota_R = Cota - 598
            dVol_dCot = (a1 + 2 * a2 * Cota_R + 3 * a3 * Cota_R ** 2) / 1000
            return dVol_dCot

        if nombre == 'HID.Colbun' or nombre=='COLBUN':
            a3 = 215.679132
            a2 = -564.993651
            a1 = 496.907289
            Cmax = 437
            Vmax = 1550.63
            dVol_COLBUN = (a1 / Cmax + (2 * a2) * (Cota / Cmax) + (3 * a3) * (Cota / Cmax) ** 2) *Vmax
            return dVol_COLBUN

    def rendimiento(self,nombre,Cota):

        if nombre == 'HID.Guaiquivilo' or nombre=='GUAIQUIVILO':
            Rend0 = -9.795508461
            cons1 = 0.0091608326
            Rend_GUAIQUIVILO = Rend0 + Cota * cons1
            return Rend_GUAIQUIVILO

        if nombre == 'HID.Cipreses' or nombre=='CIPRESES':
            cons1 = -354.62162659
            cons2 = 0.5410166883
            cons3 = -0.0002046658
            Rend_CIPRESES = cons1 + cons2 * Cota + cons3 * Cota ** 2
            return Rend_CIPRESES

        if nombre == 'HID.Canutillar' or nombre=='CANUTILLAR':
            Rend0 = 2.082606
            cons1 = -0.018920591
            cons2 = 0.000120086
            cons3 = -0.0000001742
            Rend_CANUTILLAR = Rend0 + Cota * cons1 + Cota ^ 2 * cons2 + Cota ^ 3 * cons3
            return Rend_CANUTILLAR

        if nombre== 'HID.Pangue' or nombre=='PANGUE':
            cons1 = -3.6981
            cons2 = 0.0094
            cons3 = -0.0000008
            Rend_PANGUE = cons1 + cons2 * Cota + cons3 * Cota ** 2
            return Rend_PANGUE

        if nombre== 'HID.Pehuenche' or nombre=='PEHUENCHE':
            d0 = 17.2105
            d1 = -0.0563686
            d2 = 0.0000502872
            Rend_PEHUENCHE = d0 + d1 * Cota + d2 * Cota * Cota
            return Rend_PEHUENCHE

        if nombre== 'HID.Eltoro' or nombre=='ELTORO':
            cons1 = 0.008
            cons2 = 5.931
            Rend_ELTORO = cons1 * Cota - cons2
            return Rend_ELTORO

        if nombre == 'HID.Rapel' or nombre=='RAPEL':
            cons1 = -1.18346
            cons2 = 0.026904
            cons3 = -0.00009

            Rend_RAPEL = cons1 + cons2 * Cota + cons3 * Cota ** 2
            return Rend_RAPEL

        if nombre == 'HID.Ralco' or nombre=='RALCO':
            Pend = 0.0081
            Cte0 = -4.1740612
            Rend_RALCO = Pend * Cota + Cte0
            return Rend_RALCO

        if nombre == 'HID.Colbun' or nombre=='COLBUN':
            Rend0 = 1.55
            Cota0 = 430.55
            CotaD = 267.76
            Rend_COLBUN = Rend0 * (Cota - CotaD) / (Cota0 - CotaD)
            return Rend_COLBUN

    def filtraciones(self,nombre,Cota):

        if nombre == 'HID.Cipreses' or nombre=='CIPRESES':
            if  Cota <= 1307:
                Filt_CIPRESES = 0.158 * Cota - 192.212
            else:
                Filt_CIPRESES = 0.531 * Cota - 679.985
            return Filt_CIPRESES

        if nombre == 'HID.Eltoro' or nombre=='ELTORO':
            a0 = -133471.205667
            a1 = 251.668765787
            a2 = -0.112314280288
            a3 = -0.000031180464
            a4 = 0.000000022628942
            Filt_ELTORO = a0 + (a1 * Cota) + (a2 * Cota ** 2) + (a3 * Cota ** 3) + (a4 * Cota ** 4)
            return Filt_ELTORO

        if nombre == 'HID.Colbun' or nombre=='COLBUN':
            cont=0
            if Cota >= 423:
                cont+=1
                Filt_COLBUN = 0.487507978 * Cota - 202.489444
            if Cota >=411.65 and Cota < 423:
                cont += 1
                Filt_COLBUN = 0.32644867 * Cota - 134.3795415
            if cont ==0:
                Filt_COLBUN = 0
            return Filt_COLBUN


#Funcion especial para El Toro

    def punteroA(self,nombre,m,x,Datos):

        if nombre == 'HID.Eltoro' or nombre=='ELTORO':
            j=1
            k=m
            flag=True
            while flag:
                if  (k-j)<=1:
                    i=k
                    flag=False
                else:
                    i=int((k+j)/2)
                    if x<=Datos[i]:
                        k=i
                    else:
                        j=i
            d1= x- Datos[i-1]
            d2= Datos[i]- Datos[i-1]
            dc=d1/d2
            return (i,dc)

        if nombre == 'HID.Ralco' or nombre=='RALCO':
            j = 1
            k = m
            flag = True
            while flag:
                if (k - j) <= 1:
                    i=k
                    flag=False
                else:
                    i = int((k + j) / 2)
                    if x<=Datos[i]:
                        k=i
                    else:
                        j=i
            return i


        if nombre == 'HID.Colbun' or nombre=='COLBUN':
            j = 1
            k = m
            flag = True
            while flag:
                if (k - j) <= 1:
                    i = k
                    flag = False
                else:
                    i = int((k + j) / 2)
                    if x <= Datos[i]:
                        k = i
                    else:
                        j = i
            d1 = x - Datos[i - 1]
            d2 = Datos[i] - Datos[i - 1]
            dc = d1 / d2
            return (i, dc)








