package org.openmrs.module.ugandaemrreports.definition.dataset.evaluator;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Multimap;
import org.joda.time.LocalDate;
import org.joda.time.Years;
import org.openmrs.annotation.Handler;
import org.openmrs.module.reporting.common.DateUtil;
import org.openmrs.module.reporting.dataset.DataSet;
import org.openmrs.module.reporting.dataset.DataSetRow;
import org.openmrs.module.reporting.dataset.SimpleDataSet;
import org.openmrs.module.reporting.dataset.definition.DataSetDefinition;
import org.openmrs.module.reporting.dataset.definition.evaluator.DataSetEvaluator;
import org.openmrs.module.reporting.evaluation.EvaluationContext;
import org.openmrs.module.reporting.evaluation.EvaluationException;
import org.openmrs.module.ugandaemrreports.common.*;
import org.openmrs.module.ugandaemrreports.definition.dataset.definition.ARTDatasetDefinition2019;
import org.openmrs.module.ugandaemrreports.metadata.HIVMetadata;
import org.springframework.beans.factory.annotation.Autowired;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

import static org.openmrs.module.ugandaemrreports.reports.Helper.*;

/**
 */
@Handler(supports = {ARTDatasetDefinition2019.class})
public class ARTDatasetDefinitionEvaluator2019 implements DataSetEvaluator {
    @Autowired
    private HIVMetadata hivMetadata;

    @Override
    public DataSet evaluate(DataSetDefinition dataSetDefinition, EvaluationContext context) throws EvaluationException {
        SimpleDataSet dataSet = new SimpleDataSet(dataSetDefinition, context);
        ARTDatasetDefinition2019 definition = (ARTDatasetDefinition2019) dataSetDefinition;

        Integer currentMonth = Integer.valueOf(getObsPeriod(new Date(), Enums.Period.MONTHLY));
        LocalDate localDate = StubDate.dateOf(definition.getStartDate());
        String startDate = DateUtil.formatDate(definition.getStartDate(), "yyyy-MM-dd");
        String endDate = DateUtil.formatDate(definition.getEndDate(), "yyyy-MM-dd");


        String startArtThisMonth =String.format("select person_id,DATE(value_datetime) as obs_date from obs where \n" +
                        "value_datetime between '%s' and '%s'  and concept_id = 99161 and voided = 0;",startDate,endDate);

        try {
            Connection connection = sqlConnection();

            Multimap<Integer, Date> dates = getData(connection, startArtThisMonth, "person_id", "obs_date");

            String allPatients = Joiner.on(",").join(dates.keySet());

            List<Map.Entry<Integer, Date>> entries = new ArrayList<>(convert(dates).entrySet());
            entries.sort(Comparator.comparing(Map.Entry::getValue));

            Map<Integer, List<PersonDemographics>> demographics = getPatientDemographics(connection, allPatients);

            String concepts = Joiner.on(",").join(artRegisterConcepts().values());


            String encountersBeforeArtQuery = "SELECT\n" +
                    "  e.encounter_id             AS e_id,\n" +
                    "  DATE(e.encounter_datetime) AS e_date\n" +
                    "FROM encounter e INNER JOIN obs art ON (e.patient_id = art.person_id)\n" +
                    "WHERE art.concept_id = 99161 AND art.voided = 0 AND e.voided = 0 AND e.encounter_datetime >= art.value_datetime AND\n" +
                    "      e.patient_id IN (%s)\n" +
                    "      AND encounter_type = (SELECT encounter_type_id\n" +
                    "                            FROM encounter_type\n" +
                    "                            WHERE uuid = '8d5b2be0-c2cc-11de-8d13-0010c6dffd0f')\n" +
                    "UNION ALL\n" +
                    "SELECT\n" +
                    "  e.encounter_id             AS e_id,\n" +
                    "  DATE(e.encounter_datetime) AS e_date\n" +
                    "FROM encounter e\n" +
                    "WHERE e.patient_id IN (%s) AND e.voided = 0 AND e.encounter_type = (SELECT encounter_type_id\n" +
                    "                                                 FROM encounter_type\n" +
                    "                                                 WHERE uuid = '8d5b27bc-c2cc-11de-8d13-0010c6dffd0f');";

            encountersBeforeArtQuery = encountersBeforeArtQuery.replaceAll("%s", allPatients);
            Multimap<Integer, Date> encounterData = getData(connection, encountersBeforeArtQuery, "e_id", "e_date");

            String encounters = Joiner.on(",").join(encounterData.asMap().keySet());

            String obsQuery = String.format("SELECT\n" +
                    "  person_id,\n" +
                    "  concept_id,\n" +
                    "  encounter_id,\n" +
                    "  (SELECT encounter_datetime\n" +
                    "   FROM encounter e\n" +
                    "   WHERE e.encounter_id = o.encounter_id)                                                    AS enc_date,\n" +
                    "  COALESCE(value_coded, COALESCE(DATE(value_datetime), COALESCE(value_numeric, value_text))) AS val,\n" +
                    "  CASE\n" +
                    "  WHEN (SELECT YEAR(obs_datetime) - YEAR(p.birthdate) - (RIGHT(obs_datetime, 5) < RIGHT(p.birthdate, 5))\n" +
                    "        FROM person AS p\n" +
                    "        WHERE p.person_id = o.person_id) > 10 AND value_coded = 99015\n" +
                    "    THEN '1a'\n" +
                    "  WHEN (SELECT YEAR(obs_datetime) - YEAR(p.birthdate) - (RIGHT(obs_datetime, 5) < RIGHT(p.birthdate, 5))\n" +
                    "        FROM person AS p\n" +
                    "        WHERE p.person_id = o.person_id) <= 10 AND value_coded = 99015\n" +
                    "    THEN '4a'\n" +
                    "  WHEN (SELECT YEAR(obs_datetime) - YEAR(p.birthdate) - (RIGHT(obs_datetime, 5) < RIGHT(p.birthdate, 5))\n" +
                    "        FROM person AS p\n" +
                    "        WHERE p.person_id = o.person_id) > 10 AND value_coded = 99016\n" +
                    "    THEN '1b'\n" +
                    "  WHEN (SELECT YEAR(obs_datetime) - YEAR(p.birthdate) - (RIGHT(obs_datetime, 5) < RIGHT(p.birthdate, 5))\n" +
                    "        FROM person AS p\n" +
                    "        WHERE p.person_id = o.person_id) <= 10 AND value_coded = 99016\n" +
                    "    THEN '4b'\n" +
                    "  WHEN (SELECT YEAR(obs_datetime) - YEAR(p.birthdate) - (RIGHT(obs_datetime, 5) < RIGHT(p.birthdate, 5))\n" +
                    "        FROM person AS p\n" +
                    "        WHERE p.person_id = o.person_id) > 10 AND value_coded = 99005\n" +
                    "    THEN '1c'\n" +
                    "  WHEN (SELECT YEAR(obs_datetime) - YEAR(p.birthdate) - (RIGHT(obs_datetime, 5) < RIGHT(p.birthdate, 5))\n" +
                    "        FROM person AS p\n" +
                    "        WHERE p.person_id = o.person_id) <= 10 AND value_coded = 99005\n" +
                    "    THEN '4c'\n" +
                    "  WHEN (SELECT YEAR(obs_datetime) - YEAR(p.birthdate) - (RIGHT(obs_datetime, 5) < RIGHT(p.birthdate, 5))\n" +
                    "        FROM person AS p\n" +
                    "        WHERE p.person_id = o.person_id) > 10 AND value_coded = 99006\n" +
                    "    THEN '1d'\n" +
                    "  WHEN (SELECT YEAR(obs_datetime) - YEAR(p.birthdate) - (RIGHT(obs_datetime, 5) < RIGHT(p.birthdate, 5))\n" +
                    "        FROM person AS p\n" +
                    "        WHERE p.person_id = o.person_id) <= 10 AND value_coded = 99006\n" +
                    "    THEN '4d'\n" +
                    "  WHEN (SELECT YEAR(obs_datetime) - YEAR(p.birthdate) - (RIGHT(obs_datetime, 5) < RIGHT(p.birthdate, 5))\n" +
                    "        FROM person AS p\n" +
                    "        WHERE p.person_id = o.person_id) > 10 AND value_coded = 99039\n" +
                    "    THEN '1e'\n" +
                    "\n" +
                    "  WHEN (SELECT YEAR(obs_datetime) - YEAR(p.birthdate) - (RIGHT(obs_datetime, 5) < RIGHT(p.birthdate, 5))\n" +
                    "        FROM person AS p\n" +
                    "        WHERE p.person_id = o.person_id) <= 10 AND value_coded = 99039\n" +
                    "    THEN '4j'\n" +
                    "  WHEN (SELECT YEAR(obs_datetime) - YEAR(p.birthdate) - (RIGHT(obs_datetime, 5) < RIGHT(p.birthdate, 5))\n" +
                    "        FROM person AS p\n" +
                    "        WHERE p.person_id = o.person_id) > 10 AND value_coded = 99040\n" +
                    "    THEN '1f'\n" +
                    "\n" +
                    "  WHEN (SELECT YEAR(obs_datetime) - YEAR(p.birthdate) - (RIGHT(obs_datetime, 5) < RIGHT(p.birthdate, 5))\n" +
                    "        FROM person AS p\n" +
                    "        WHERE p.person_id = o.person_id) <= 10 AND value_coded = 99040\n" +
                    "    THEN '4i'\n" +
                    "  WHEN value_coded = 99041\n" +
                    "    THEN '1g'\n" +
                    "  WHEN value_coded = 99042\n" +
                    "    THEN '1h'\n" +
                    "  WHEN value_coded = 99007\n" +
                    "    THEN '2a2'\n" +
                    "  WHEN value_coded = 99008\n" +
                    "    THEN '2a4'\n" +
                    "  WHEN (SELECT YEAR(obs_datetime) - YEAR(p.birthdate) - (RIGHT(obs_datetime, 5) < RIGHT(p.birthdate, 5))\n" +
                    "        FROM person AS p\n" +
                    "        WHERE p.person_id = o.person_id) > 10 AND value_coded = 99044\n" +
                    "    THEN '2b'\n" +
                    "  WHEN (SELECT YEAR(obs_datetime) - YEAR(p.birthdate) - (RIGHT(obs_datetime, 5) < RIGHT(p.birthdate, 5))\n" +
                    "        FROM person AS p\n" +
                    "        WHERE p.person_id = o.person_id) <= 10 AND value_coded = 99044\n" +
                    "    THEN '5d'\n" +
                    "  WHEN value_coded = 99043\n" +
                    "    THEN '2c'\n" +
                    "  WHEN value_coded = 99282\n" +
                    "    THEN '2d2'\n" +
                    "  WHEN value_coded = 99283\n" +
                    "    THEN '2d4'\n" +
                    "  WHEN (SELECT YEAR(obs_datetime) - YEAR(p.birthdate) - (RIGHT(obs_datetime, 5) < RIGHT(p.birthdate, 5))\n" +
                    "        FROM person AS p\n" +
                    "        WHERE p.person_id = o.person_id) > 10 AND value_coded = 99046\n" +
                    "    THEN '2e'\n" +
                    "  WHEN (SELECT YEAR(obs_datetime) - YEAR(p.birthdate) - (RIGHT(obs_datetime, 5) < RIGHT(p.birthdate, 5))\n" +
                    "        FROM person AS p\n" +
                    "        WHERE p.person_id = o.person_id) <= 10 AND value_coded = 99046\n" +
                    "    THEN '5l'\n" +
                    "  WHEN value_coded = 99017\n" +
                    "    THEN '5a'\n" +
                    "  WHEN value_coded = 99018\n" +
                    "    THEN '5b'\n" +
                    "  WHEN value_coded = 99045\n" +
                    "    THEN '5f'\n" +
                    "  WHEN value_coded = 99284\n" +
                    "    THEN '5g'\n" +
                    "  WHEN value_coded = 99285\n" +
                    "    THEN '5h'\n" +
                    "  WHEN (SELECT YEAR(obs_datetime) - YEAR(p.birthdate) - (RIGHT(obs_datetime, 5) < RIGHT(p.birthdate, 5))\n" +
                    "        FROM person AS p\n" +
                    "        WHERE p.person_id = o.person_id) > 10 AND value_coded = 99286\n" +
                    "    THEN '2c'\n" +
                    "  WHEN (SELECT YEAR(obs_datetime) - YEAR(p.birthdate) - (RIGHT(obs_datetime, 5) < RIGHT(p.birthdate, 5))\n" +
                    "        FROM person AS p\n" +
                    "        WHERE p.person_id = o.person_id) <= 10 AND value_coded = 99286\n" +
                    "    THEN '5l'\n" +
                    "  WHEN value_coded = 99884\n" +
                    "    THEN '4e'\n" +
                    "  WHEN value_coded = 99885\n" +
                    "    THEN '4f'\n" +
                    "  WHEN value_coded = 99888\n" +
                    "    THEN '2h'\n" +
                    "  WHEN value_coded = 163017\n" +
                    "    THEN '2g'\n" +
                    "  WHEN value_coded = 90002\n" +
                    "    THEN 'othr'\n" +
                    "  WHEN value_coded IN (90033, 90079, 1204)\n" +
                    "    THEN '1'\n" +
                    "  WHEN value_coded IN (90034, 90073, 1205)\n" +
                    "    THEN '2'\n" +
                    "  WHEN value_coded IN (90035, 90078, 1206)\n" +
                    "    THEN '3'\n" +
                    "  WHEN value_coded IN (90036, 90071, 1207)\n" +
                    "    THEN '4'\n" +
                    "  WHEN value_coded = 90293\n" +
                    "    THEN 'T1'\n" +
                    "  WHEN value_coded = 90294\n" +
                    "    THEN 'T2'\n" +
                    "  WHEN value_coded = 90295\n" +
                    "    THEN 'T3'\n" +
                    "  WHEN value_coded = 90295\n" +
                    "    THEN 'T4'\n" +
                    "  WHEN value_coded = 90156\n" +
                    "    THEN 'G'\n" +
                    "  WHEN value_coded = 90157\n" +
                    "    THEN 'F'\n" +
                    "  WHEN value_coded = 90158\n" +
                    "    THEN 'P'\n" +
                    "  WHEN value_coded = 90003\n" +
                    "    THEN 'Y'\n" +
                    "  ELSE ''\n" +
                    "  END                                                                                        AS report_name\n" +
                    "FROM obs o\n" +
                    "WHERE o.voided = 0 AND o.encounter_id IN (%s) AND o.concept_id IN (%s)\n" +
                    "UNION ALL\n" +
                    "SELECT\n" +
                    "  p.person_id,\n" +
                    "  0,\n" +
                    "  0,\n" +
                    "  death_date,\n" +
                    "  DATE(death_date),\n" +
                    "  ''\n" +
                    "FROM person p INNER JOIN obs art ON (p.person_id = art.person_id)\n" +
                    "WHERE art.concept_id = 99161 AND p.person_id IN (%s) AND art.voided = 0 AND p.voided = 0 AND p.death_date >= art.value_datetime;", encounters, concepts, allPatients);

            List<ObsData> table = getData(connection, obsQuery);

            PatientDataHelper pdh = new PatientDataHelper();

            for (Map.Entry<Integer, Date> patient : entries) {
                DataSetRow row = new DataSetRow();

                Integer key = patient.getKey();

                List<PersonDemographics> personDemographics = demographics.get(key);

                List<ObsData> patientData = table.stream()
                        .filter(line -> line.getPatientId().compareTo(key) == 0)
                        .collect(Collectors.toList());

                ObsData artStartDate = getData(patientData, "99161");
                ObsData tbStartDate = getData(patientData, "90217");
                ObsData tbStopDate = getData(patientData, "90310");
                ObsData ti = getData(patientData, "99160");
                ObsData baselineWeight = getData(patientData, "99069");
                ObsData height = getData(patientData, "5090");
                ObsData baselineCs = getData(patientData, "99070");
                ObsData baselineCd4 = getData(patientData, "99071");
                ObsData baselineRegimen = getData(patientData, "99061");
                ObsData careEntryPoint = getData(patientData, "90200");
                ObsData specialCategory = getData(patientData, "165169");
                ObsData advDisease = getData(patientData, "165272");
                ObsData muacCode = getData(patientData, "99030");
                ObsData muac = getData(patientData, "1343");
                ObsData nutritional = getData(patientData, "165050");
                ObsData crag = getData(patientData, "165418");
                ObsData TBLam = getData(patientData, "165416");
                ObsData HepBResults = getFirstData(patientData, "1322");
                ObsData SyphillisResults = getFirstData(patientData, "99752");
                ObsData TPTStartDate = getData(patientData, "165226");
                ObsData TPTStopDate = getData(patientData, "165227");
                ObsData fluconazoleStartDate = getData(patientData, "1190");
                ObsData fluconazoleStopDate = getData(patientData, "1191");
                ObsData TBStatus= getData(patientData, "90216");


                boolean hasDied = false;
                boolean hasTransferred = false;

                String startedTB = tbStartDate != null ? DateUtil.formatDate(DateUtil.parseYmd(tbStartDate.getVal()), "MM/yyyy") : "";
                String stoppedTB = tbStopDate != null ? DateUtil.formatDate(DateUtil.parseYmd(tbStopDate.getVal()), "MM/yyyy") : "";
                String startedFLuc =  fluconazoleStartDate != null ? DateUtil.formatDate(DateUtil.parseYmd(fluconazoleStartDate.getVal()), "MM/yyyy") : "";
                String stoppedFluc = fluconazoleStopDate != null ? DateUtil.formatDate(DateUtil.parseYmd(fluconazoleStopDate.getVal()), "MM/yyyy") : "";


                PersonDemographics personDemos = personDemographics != null && personDemographics.size() > 0 ? personDemographics.get(0) : new PersonDemographics();

                List<String> addresses = processString2(personDemos.getAddresses());

                pdh.addCol(row, "Date ART Started", patient.getValue());
                pdh.addCol(row, "Unique ID no", "");
                pdh.addCol(row, "TI", ti == null ? "" : "TI");
                pdh.addCol(row, "emtct", !careEntryPoint.getVal().equals("90012")? "": "EMTCT" );
                pdh.addCol(row, "Patient Clinic ID", processString(personDemos.getIdentifiers()).get("e1731641-30ab-102d-86b0-7a5022ba4115"));
                pdh.addCol(row, "NIN", processString(personDemos.getIdentifiers()).get("f0c16a6d-dc5f-4118-a803-616d0075d282"));
                pdh.addCol(row, "clientCategory", processString(personDemos.getAttributes()).get("dec484be-1c43-416a-9ad0-18bd9ef28929"));

                List<String> names = Splitter.on(" ").splitToList(personDemos.getNames());

                pdh.addCol(row, "Surname", names.size() > 0 ? names.get(0) : "");
                pdh.addCol(row, "GivenName", names.size() > 1 ? names.get(1) : "");
                pdh.addCol(row, "Gender", personDemos.getGender());
                if (personDemos.getBirthDate() != null && artStartDate != null) {
                    Years age = Years.yearsBetween(StubDate.dateOf(personDemos.getBirthDate()), StubDate.dateOf(patient.getValue()));
                    pdh.addCol(row, "Age", age.getYears());
                } else {
                    pdh.addCol(row, "Age", "");
                }

                if (addresses.size() == 6) {
                    pdh.addCol(row, "District", addresses.get(1));
                    pdh.addCol(row, "Subcounty", addresses.get(3));
                    pdh.addCol(row, "parish", addresses.get(4));
                    pdh.addCol(row, "Village/Cell", addresses.get(5));

                } else {
                    pdh.addCol(row, "District", "");
                    pdh.addCol(row, "Subcounty", "");
                    pdh.addCol(row, "parish", "");
                    pdh.addCol(row, "Village/Cell", "");
                }
                pdh.addCol(row, "Weight", baselineWeight == null ? "" : baselineWeight.getVal());
                pdh.addCol(row, "height", height == null ? "" : height.getVal());

                ObsData functionalStatusDuringArtStart = getFirstData(patientData, "90235");

                ObsData firstCPT = getFirstData(patientData, "99037");
                ObsData firstINH = getFirstData(patientData, "99604");
                List<ObsData> viralLoads = getDataAsList(patientData, "856");

                ObsData firstViralLoad = viralLoad(viralLoads, 6);

                String fvl = "";

                if (firstViralLoad != null) {
                    fvl = firstViralLoad.getVal();
                }

                if (functionalStatusDuringArtStart != null) {
                    pdh.addCol(row, "FUS", functionalStatusDuringArtStart.getReportName());
                } else {
                    pdh.addCol(row, "FUS", "");
                }


                if (baselineCs != null) {
                    pdh.addCol(row, "cStage", baselineCs.getReportName());
                } else {
                    pdh.addCol(row, "cStage", "");
                }

                if(advDisease !=null){
                    pdh.addCol(row, "advDisease", advDisease.getVal());
                }else{
                    pdh.addCol(row, "advDisease", "");
                }

                if(specialCategory !=null){
                    pdh.addCol(row, "specialCategory", specialCategory.getVal());
                }else{
                    pdh.addCol(row, "specialCategory", "");
                }


                pdh.addCol(row, "crAg", crag == null ? "" : crag.getVal());
                pdh.addCol(row, "TB LAM", TBLam == null ? "" : TBLam.getVal());
                pdh.addCol(row, "hepBtesting", HepBResults == null ? "N" : "Y");
                pdh.addCol(row, "hepResults", HepBResults == null ? "" : HepBResults.getVal());
                pdh.addCol(row, "Syphilis", SyphillisResults == null ? "" : SyphillisResults.getVal());
                pdh.addCol(row, "TPT Start Date", TPTStartDate == null ? "" : TPTStartDate.getVal());
                pdh.addCol(row, "TPT Stop Date", TPTStopDate == null ? "" : TPTStopDate.getVal());
                pdh.addCol(row, "Fluc Start Date",  startedFLuc);
                pdh.addCol(row, "Fluc Stop Date",  stoppedFluc);

                pdh.addCol(row, "nutritional", nutritional == null ? "" : nutritional.getVal());
                pdh.addCol(row, "colorCode", muacCode == null ? "" : muacCode.getVal());
                pdh.addCol(row, "MUAC", muac == null ? "" : muac.getVal());
                pdh.addCol(row, "CD4", baselineCd4 == null ? "" : baselineCd4.getVal());
                pdh.addCol(row, "VL", fvl);

                pdh.addCol(row, "CPT Start Date", firstCPT == null ? "" : DateUtil.formatDate(firstCPT.getEncounterDate(), "MM/yyyy"));
                pdh.addCol(row, "CPT Stop Date", "");
                pdh.addCol(row, "INH Start Date", firstINH == null ? "" : DateUtil.formatDate(firstINH.getEncounterDate(), "MM/yyyy"));
                pdh.addCol(row, "INH Stop Date", "");
                pdh.addCol(row, "TB Reg No", "");
                pdh.addCol(row, "TBstartDate", startedTB);
                pdh.addCol(row, "TB Stop Date", stoppedTB);
                pdh.addCol(row, "TB status", TBStatus == null ? "" : TBStatus.getVal());

                pdh.addCol(row, "EDD1", "");
                pdh.addCol(row, "ANC1", "");
                pdh.addCol(row, "INFANT1", "");

                pdh.addCol(row, "EDD2", "");
                pdh.addCol(row, "ANC2", "");
                pdh.addCol(row, "INFANT2", "");

                pdh.addCol(row, "EDD3", "");
                pdh.addCol(row, "ANC3", "");
                pdh.addCol(row, "INFANT3", "");

                pdh.addCol(row, "EDD4", "");
                pdh.addCol(row, "ANC4", "");
                pdh.addCol(row, "INFANT4", "");

                if (baselineRegimen != null) {
                    pdh.addCol(row, "BASE REGIMEN", baselineRegimen.getReportName());
                } else {
                    pdh.addCol(row, "BASE REGIMEN", "");
                }

                pdh.addCol(row, "L1S1", "");
                pdh.addCol(row, "L1S2", "");
                pdh.addCol(row, "L1S3", "");
                pdh.addCol(row, "L1S4", "");
                pdh.addCol(row, "L2S1", "");
                pdh.addCol(row, "L2S2", "");
                pdh.addCol(row, "L2S3", "");
                pdh.addCol(row, "L2S4", "");
                pdh.addCol(row, "L3S1", "");
                pdh.addCol(row, "L3S2", "");
                pdh.addCol(row, "L3S3", "");
                pdh.addCol(row, "L3S4", "");
                pdh.addCol(row, "Patient Clinic ID", processString(personDemos.getIdentifiers()).get("e1731641-30ab-102d-86b0-7a5022ba4115"));

                ObsData visit = null;

                for (int i = 0; i <= 72; i++) {
                    String workingMonth = getObsPeriod(Periods.addMonths(localDate, i).get(0).toDate(), Enums.Period.MONTHLY);
                    Integer period = Integer.valueOf(workingMonth);

                    ObsData currentEncounter = getData(patientData, period);

                    if (period <= currentMonth && (!hasDied || !hasTransferred)) {

                        ObsData tbStatus = getData(patientData, workingMonth, "90216");
                        ObsData arvAdh = getData(patientData, workingMonth, "90221");

                        ObsData inhDosage = getData(patientData, workingMonth, "99604");
                        ObsData cptDosage = getData(patientData, workingMonth, "99037");

                        ObsData currentRegimen = getData(patientData, workingMonth, "90315");
                        ObsData returnDate = getData(patientData, workingMonth, "5096");

                        ObsData arvStopDate = getData(patientData, workingMonth, "99084");
                        ObsData arvRestartDate = getData(patientData, workingMonth, "99085");

                        ObsData toDate = getData(patientData, workingMonth, "99165");
                        ObsData currentlyDead = getData(patientData, workingMonth, "deaths");
                        ObsData ARTCode = getData(patientData, workingMonth, "90315");
                        ObsData DSDMCode = getData(patientData, workingMonth, "165143");
                        ObsData TPT_Status = getData(patientData, workingMonth, "165288");
                        ObsData Nutritional_Status = getData(patientData, workingMonth, "165050");
                        ObsData advanced_Disease = getData(patientData, workingMonth, "165272");
                        ObsData BF_Pregnacy_Status = getData(patientData, workingMonth, "90041");
                        ObsData viralLoad = viralLoad(viralLoads, i);


                        if (returnDate != null) {
                            visit = returnDate;
                        }

                        String cotrim = "";
                        String status = "";
                        String adherence = "";
                        String tb = "";
                        String ART_Code ="";
                        String TPTStatus="";
                        String nutritionStatus="";
                        String adv_Disease_status ="";
                        String DSDM= "";
                        String VL_Status="";
                        String pregnancy_status="";


                        if (inhDosage != null || cptDosage != null) {
                            cotrim = "Y";
                        }
                        if (currentRegimen != null) {
                            status = currentRegimen.getReportName();
                        } else if (returnDate != null) {
                            status = "3";
                        } else if (currentEncounter != null) {
                            status = "=UNICHAR(8730)";
                        } else {
                            if (arvStopDate != null) {
                                status = "2";
                            } else if (arvRestartDate != null) {
                                status = "6";
                            } else if (currentlyDead != null) {
                                status = "1";
                                hasDied = true;
                            } else if (toDate != null) {
                                status = "5";
                                hasTransferred = true;
                            } else {
                                if (visit != null) {
                                    Integer appointmentPeriod = Integer.parseInt(getObsPeriod(DateUtil.parseYmd(visit.getVal()), Enums.Period.MONTHLY));
                                    Integer diff = period - appointmentPeriod;
                                    if (diff <= 0) {
                                        status = "=UNICHAR(8594)";
                                    } else if (diff < 3) {
                                        status = "3";
                                    } else {
                                        status = "4";
                                    }
                                }
                            }
                        }

                        if (tbStatus != null) {
                            tb = tbStatus.getReportName();
                        }
                        if (arvAdh != null) {
                            adherence = arvAdh.getReportName();
                        }

                        if(ARTCode !=null){
                            ART_Code = ARTCode.getVal();
                        }
                        if(DSDMCode !=null){
                            DSDM = DSDMCode.getVal();
                        }
                        if(TPT_Status !=null){
                            TPTStatus = TPT_Status.getVal();
                        }
                        if(Nutritional_Status !=null){
                            nutritionStatus = Nutritional_Status.getVal();
                        }
                        if(advanced_Disease !=null){
                            adv_Disease_status =advanced_Disease.getVal();
                        }
                        if(BF_Pregnacy_Status !=null){
                             pregnancy_status=BF_Pregnacy_Status.getVal();
                        }


                        pdh.addCol(row, "fu status" + String.valueOf(i), status);
                        pdh.addCol(row, "TB Status" + String.valueOf(i), tb);
                        pdh.addCol(row, "ADH" + String.valueOf(i), adherence);
                        pdh.addCol(row, "CTX" + String.valueOf(i), cotrim);

                        pdh.addCol(row, "ARV Code" + String.valueOf(i), ART_Code);
                        pdh.addCol(row, "TPT" + String.valueOf(i), TPTStatus);
                        pdh.addCol(row, "NutriStatus" + String.valueOf(i), nutritionStatus);
                        pdh.addCol(row, "adv Disease" + String.valueOf(i), adv_Disease_status);
                        pdh.addCol(row, "Preg/BFStatus" + String.valueOf(i), pregnancy_status);
                        pdh.addCol(row, "DSDM" + String.valueOf(i), DSDM);
                        pdh.addCol(row, "VL Status" + String.valueOf(i), (viralLoad == null ? "" : viralLoad.getVal()));


                        if (i == 6 || i == 12 || i == 18 || i == 24 || i == 30 || i == 36 || i == 42 ||i == 48 || i == 54|| i == 60 || i == 66 || i == 72) {
                            ObsData weight = getData(patientData, workingMonth, "90236");
                            ObsData cd4 = getData(patientData, workingMonth, "5497");
                            ObsData clinicalStage = getData(patientData, workingMonth, "90203");
                            ObsData depressionStatus = getData(patientData, workingMonth, "165194");
                            ObsData discloseStatus = getData(patientData, workingMonth, "99175");
                            ObsData CaCx = getData(patientData, workingMonth, "165315");


                            pdh.addCol(row, "CI" + String.valueOf(i), clinicalStage == null ? "" : clinicalStage.getReportName());

                            pdh.addCol(row, "weight" + String.valueOf(i), weight == null ? "" : weight.getVal());

                            pdh.addCol(row, "CD4" + String.valueOf(i), (cd4 == null ? "" : cd4.getVal()));

                            pdh.addCol(row, "Dep status" + String.valueOf(i), (depressionStatus == null ? "" : depressionStatus.getVal()));

                            pdh.addCol(row, "Disc Status" + String.valueOf(i), (discloseStatus == null ? "" : discloseStatus.getVal()));

                            pdh.addCol(row, "CaCx" + String.valueOf(i), (CaCx == null ? "" : CaCx.getVal()));
                        }
                    } else {
                        pdh.addCol(row, "fu status" + String.valueOf(i), "");
                        pdh.addCol(row, "TB Status" + String.valueOf(i), "");
                        pdh.addCol(row, "ADH" + String.valueOf(i), "");
                        pdh.addCol(row, "CTX" + String.valueOf(i), "");

                        pdh.addCol(row, "ARV Code" + String.valueOf(i), "");
                        pdh.addCol(row, "TPT" + String.valueOf(i), "");
                        pdh.addCol(row, "NutriStatus" + String.valueOf(i), "");
                        pdh.addCol(row, "adv Disease" + String.valueOf(i), "");
                        pdh.addCol(row, "Preg/BFStatus" + String.valueOf(i), "");
                        pdh.addCol(row, "DSDM" + String.valueOf(i), "");
                        pdh.addCol(row, "VL Status" + String.valueOf(i), "");
                        if (i == 6 || i == 12 || i == 18 || i == 24 || i == 30 || i == 36 || i == 42 ||i == 48 || i == 54|| i == 60 || i == 66 || i == 72) {
                            pdh.addCol(row, "CI" + String.valueOf(i), "");
                            pdh.addCol(row, "weight" + String.valueOf(i), "");
                            pdh.addCol(row, "CD4" + String.valueOf(i), "");
                            pdh.addCol(row, "Dep status" + String.valueOf(i), "");
                            pdh.addCol(row, "Disc Status" + String.valueOf(i), "");
                            pdh.addCol(row, "CaCx" + String.valueOf(i), "");

                        }
                    }

                }
                dataSet.addRow(row);
            }
        } catch (SQLException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        return dataSet;
    }
}
