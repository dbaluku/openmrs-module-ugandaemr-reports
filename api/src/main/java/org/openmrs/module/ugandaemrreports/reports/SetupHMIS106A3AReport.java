package org.openmrs.module.ugandaemrreports.reports;

import org.openmrs.module.reporting.ReportingConstants;
import org.openmrs.module.reporting.cohort.definition.BaseObsCohortDefinition;
import org.openmrs.module.reporting.cohort.definition.CohortDefinition;
import org.openmrs.module.reporting.dataset.definition.CohortIndicatorDataSetDefinition;
import org.openmrs.module.reporting.evaluation.parameter.Mapped;
import org.openmrs.module.reporting.evaluation.parameter.Parameter;
import org.openmrs.module.reporting.indicator.CohortIndicator;
import org.openmrs.module.reporting.indicator.dimension.CohortDefinitionDimension;
import org.openmrs.module.reporting.report.ReportDesign;
import org.openmrs.module.reporting.report.definition.ReportDefinition;
import org.openmrs.module.ugandaemrreports.library.*;
import org.openmrs.module.ugandaemrreports.metadata.HIVMetadata;
import org.openmrs.module.ugandaemrreports.metadata.TBMetadata;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.openmrs.module.ugandaemrreports.reporting.metadata.Dictionary.getConcept;

/**
 *  TX Current Report
 */
@Component
public class SetupHMIS106A3AReport extends UgandaEMRDataExportManager {

    @Autowired
    private DataFactory df;

    @Autowired
    SetupMERTxNew2019Report setupTxNewReport;

    @Autowired
    TBCohortDefinitionLibrary tbCohortDefinitionLibrary;

    @Autowired
    HIVCohortDefinitionLibrary hivCohortDefinitionLibrary;

    @Autowired
    CommonDimensionLibrary commonDimensionLibrary;

    @Autowired
    private TBMetadata tbMetadata;

    @Autowired
    private HIVMetadata hivMetadata;

    @Autowired
    private CommonCohortDefinitionLibrary cohortDefinitionLibrary;



    /**
     * @return the uuid for the report design for exporting to Excel
     */
    @Override
    public String getExcelDesignUuid() {
        return "220e7492-e56d-4f99-a424-40eda06668f2";
    }

    @Override
    public String getUuid() {
        return "dc69d1a2-2f2e-4512-896e-44c6bf7d1c42";
    }

    @Override
    public String getName() {
        return "HMIS 1061a 3A Report";
    }

    @Override
    public String getDescription() {
        return "HMIS 1061a 3A Report ";
    }

    @Override
    public List<Parameter> getParameters() {
        List<Parameter> l = new ArrayList<Parameter>();
        l.add(df.getStartDateParameter());
        l.add(df.getEndDateParameter());
        return l;
    }


    @Override
    public List<ReportDesign> constructReportDesigns(ReportDefinition reportDefinition) {
        return Arrays.asList(buildReportDesign(reportDefinition));
    }

    /**
     * Build the report design for the specified report, this allows a user to override the report design by adding
     * properties and other metadata to the report design
     *
     * @param reportDefinition
     * @return The report design
     */
    @Override
    public ReportDesign buildReportDesign(ReportDefinition reportDefinition) {
        return createExcelTemplateDesign(getExcelDesignUuid(), reportDefinition, "106A3_1.xls");
    }



    @Override
    public ReportDefinition constructReportDefinition() {

        ReportDefinition rd = new ReportDefinition();

        rd.setUuid(getUuid());
        rd.setName(getName());
        rd.setDescription(getDescription());
        rd.setParameters(getParameters());

        CohortIndicatorDataSetDefinition dsd = new CohortIndicatorDataSetDefinition();
        CohortIndicatorDataSetDefinition dsd1 = new CohortIndicatorDataSetDefinition();
        CohortIndicatorDataSetDefinition dsd2 = new CohortIndicatorDataSetDefinition();
        CohortIndicatorDataSetDefinition dsd3 = new CohortIndicatorDataSetDefinition();
        CohortIndicatorDataSetDefinition dsd4 = new CohortIndicatorDataSetDefinition();
        CohortIndicatorDataSetDefinition dsd5 = new CohortIndicatorDataSetDefinition();
        CohortIndicatorDataSetDefinition dsd6 = new CohortIndicatorDataSetDefinition();
        CohortIndicatorDataSetDefinition dsd7 = new CohortIndicatorDataSetDefinition();
        dsd.setParameters(getParameters());
        dsd1.setParameters(getParameters());
        dsd2.setParameters(getParameters());
        dsd3.setParameters(getParameters());
        dsd4.setParameters(getParameters());
        dsd5.setParameters(getParameters());
        dsd6.setParameters(getParameters());
        dsd7.setParameters(getParameters());


        CohortDefinitionDimension patientTypeDimensions = commonDimensionLibrary.getPatientTypeDimension();


        CohortDefinitionDimension ageDimension = commonDimensionLibrary.getTxCurrentAgeGenderGroup();
        dsd2.addDimension("age", Mapped.mapStraightThrough(ageDimension));
        dsd3.addDimension("age", Mapped.mapStraightThrough(ageDimension));



        dsd.addDimension("type", Mapped.mapStraightThrough(patientTypeDimensions));
        dsd1.addDimension("type", Mapped.mapStraightThrough(patientTypeDimensions));

        rd.addDataSetDefinition("A1", Mapped.mapStraightThrough(dsd));
        rd.addDataSetDefinition("A2", Mapped.mapStraightThrough(dsd1));
        rd.addDataSetDefinition("B", Mapped.mapStraightThrough(dsd2));
        rd.addDataSetDefinition("C", Mapped.mapStraightThrough(dsd3));
        rd.addDataSetDefinition("D", Mapped.mapStraightThrough(dsd4));
        rd.addDataSetDefinition("E", Mapped.mapStraightThrough(dsd5));
        rd.addDataSetDefinition("G", Mapped.mapStraightThrough(dsd6));
        rd.addDataSetDefinition("H", Mapped.mapStraightThrough(dsd7));

        CohortDefinition males = cohortDefinitionLibrary.males();
        CohortDefinition females = cohortDefinitionLibrary.females();


        CohortDefinition above15Years = cohortDefinitionLibrary.above15Years();
        CohortDefinition below15Years = cohortDefinitionLibrary.MoHChildren();

        CohortDefinition registered = tbCohortDefinitionLibrary.getEnrolledOnDSTBDuringPeriod();
        CohortDefinition startedOnTBTreatmentDuringPeriod = tbCohortDefinitionLibrary.getPatientsStartedOnTreatmentDuringperiod();

        CohortDefinition bacteriologicallyConfirmed = df.getPatientsWithCodedObsDuringPeriod(tbMetadata.getPatientType(),tbMetadata.getTBEnrollmentEncounterType(),Arrays.asList(tbMetadata.getBacteriologicallyConfirmed()), BaseObsCohortDefinition.TimeModifier.ANY);
        CohortDefinition clinicallyConfirmed = df.getPatientsWithCodedObsDuringPeriod(tbMetadata.getPatientType(),tbMetadata.getTBEnrollmentEncounterType(),Arrays.asList(tbMetadata.getClinicallyDiagnosed()), BaseObsCohortDefinition.TimeModifier.ANY);
        CohortDefinition EPTBConfirmed = df.getPatientsWithCodedObsDuringPeriod(tbMetadata.getPatientType(),tbMetadata.getTBEnrollmentEncounterType(),Arrays.asList(tbMetadata.getEPTB()), BaseObsCohortDefinition.TimeModifier.ANY);

        CohortDefinition bacteriologicallyConfirmedAndRegistered = df.getPatientsInAll(registered,bacteriologicallyConfirmed);
        CohortDefinition bacteriologicallyConfirmedAndStartedOnTratment = df.getPatientsInAll(startedOnTBTreatmentDuringPeriod,bacteriologicallyConfirmed);

        CohortDefinition clinicallyConfirmedAndRegistered = df.getPatientsInAll(registered,clinicallyConfirmed);
        CohortDefinition clinicallyConfirmedAndStartedOnTratment = df.getPatientsInAll(startedOnTBTreatmentDuringPeriod,clinicallyConfirmed);

        CohortDefinition EPTBConfirmedAndRegistered = df.getPatientsInAll(registered,EPTBConfirmed);
        CohortDefinition EPTBConfirmedAndStartedOnTratment = df.getPatientsInAll(startedOnTBTreatmentDuringPeriod,EPTBConfirmed);

        CohortDefinition newAndRelapsedPatients = tbCohortDefinitionLibrary.getNewAndRelapsedPatientsDuringPeriod();
        CohortDefinition newAndRelapsedRegisteredClients = df.getPatientsInAll(newAndRelapsedPatients,registered);

        CohortDefinition HIVStatusNewlyDocumented = tbCohortDefinitionLibrary.getPatientsWhoseHIVStatusIsNewlyDocumented();
        CohortDefinition newAndRelapsedPatientsWhoHaveHIVStatusNewlyDocumented= df.getPatientsInAll(newAndRelapsedPatients,HIVStatusNewlyDocumented);

        CohortDefinition newlyDiagnosedHIVPositive = tbCohortDefinitionLibrary.getPatientsWhoseHIVStatusIsNewlyPositive();
        CohortDefinition knownHIVPositive = tbCohortDefinitionLibrary.getPatientsWhoseHIVStatusIsKnownPositive();
        CohortDefinition newAndRelapsedPatientsWhoHaveKnownHIVPositive = df.getPatientsInAll(knownHIVPositive,newAndRelapsedPatients);

        CohortDefinition initiatedOnCPTDuringPeriod = tbCohortDefinitionLibrary.getPatientsOnCPTOnTBEnrollment();
        CohortDefinition initiatedOnARTDuringPeriod = tbCohortDefinitionLibrary.getPatientsStartedOnARTOnTBEnrollment();

        CohortDefinition noOfPatientsWithTreatmentSupporters =tbCohortDefinitionLibrary.getPatientsWithTreatmentSupporters();
        CohortDefinition patientsWhoAreHealthWorkers = df.getPatientsWithCodedObsDuringPeriod(tbMetadata.getRiskGroup(),tbMetadata.getTBEnrollmentEncounterType(),Arrays.asList(getConcept("5619AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")), BaseObsCohortDefinition.TimeModifier.ANY);
        CohortDefinition patientsWhoAreTBContacts = df.getPatientsWithCodedObsDuringPeriod(tbMetadata.getRiskGroup(),tbMetadata.getTBEnrollmentEncounterType(),Arrays.asList(getConcept("b5171d08-77bf-40a8-a864-51caa6cd2480")), BaseObsCohortDefinition.TimeModifier.ANY);
        CohortDefinition patientsWhoAreRefugees = df.getPatientsWithCodedObsDuringPeriod(tbMetadata.getRiskGroup(),tbMetadata.getTBEnrollmentEncounterType(),Arrays.asList(getConcept("165127AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")), BaseObsCohortDefinition.TimeModifier.ANY);
        CohortDefinition patientsWhoArePrisoners = df.getPatientsWithCodedObsDuringPeriod(tbMetadata.getRiskGroup(),tbMetadata.getTBEnrollmentEncounterType(),Arrays.asList(getConcept("162277AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")), BaseObsCohortDefinition.TimeModifier.ANY);
        CohortDefinition patientsWhoAreUniformedPeople = df.getPatientsWithCodedObsDuringPeriod(tbMetadata.getRiskGroup(),tbMetadata.getTBEnrollmentEncounterType(),Arrays.asList(getConcept("165125AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")), BaseObsCohortDefinition.TimeModifier.ANY);
        CohortDefinition patientsWhoAreFisherMen = df.getPatientsWithCodedObsDuringPeriod(tbMetadata.getRiskGroup(),tbMetadata.getTBEnrollmentEncounterType(),Arrays.asList(getConcept("159674AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")), BaseObsCohortDefinition.TimeModifier.ANY);
        CohortDefinition patientsWhoAreDiabetic = df.getPatientsWithCodedObsDuringPeriod(tbMetadata.getRiskGroup(),tbMetadata.getTBEnrollmentEncounterType(),Arrays.asList(getConcept("119481AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")), BaseObsCohortDefinition.TimeModifier.ANY);
        CohortDefinition patientsWhoAreMiners = df.getPatientsWithCodedObsDuringPeriod(tbMetadata.getRiskGroup(),tbMetadata.getTBEnrollmentEncounterType(),Arrays.asList(getConcept("952c6973-e163-4c0d-b6c8-a7071bd05e2a")), BaseObsCohortDefinition.TimeModifier.ANY);
        CohortDefinition patientsWhoAreSmokers = df.getPatientsWithCodedObsDuringPeriod(tbMetadata.getRiskGroup(),tbMetadata.getTBEnrollmentEncounterType(),Arrays.asList(getConcept("1455AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")), BaseObsCohortDefinition.TimeModifier.ANY);
        CohortDefinition patientsWhoAreMentallyIll = df.getPatientsWithCodedObsDuringPeriod(tbMetadata.getRiskGroup(),tbMetadata.getTBEnrollmentEncounterType(),Arrays.asList(getConcept("134337AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")), BaseObsCohortDefinition.TimeModifier.ANY);

        CohortDefinition transferredInTheQuarter = hivCohortDefinitionLibrary.getTransferredInToCareDuringPeriod();

        CohortDefinition havingArtStartDateDuringQuarter = hivCohortDefinitionLibrary.getArtStartDateBetweenPeriod();
        CohortDefinition havingArtStartDateBeforeQuarter = hivCohortDefinitionLibrary.getArtStartDateBeforePeriod();
        CohortDefinition clientsStartedOnARTAtThisFacilityDuringPeriod = df.getPatientsNotIn(havingArtStartDateDuringQuarter,transferredInTheQuarter);
        CohortDefinition clientsStartedOnARTAtThisFacilityBeforePeriod = df.getPatientsNotIn(havingArtStartDateBeforeQuarter,hivCohortDefinitionLibrary.getPatientsTransferredOutDuringPeriod());
        CohortDefinition noSignsOfTBDuringPeriod =  hivCohortDefinitionLibrary.getScreenedForTBNegativeDuringPeriod();
        CohortDefinition startedTPTDuringQuarter = df.getPatientsWhoseObsValueDateIsBetweenStartDateAndEndDate(hivMetadata.getConcept("483939c7-79ba-4ca4-8c3e-346488c97fc7"),hivMetadata.getARTSummaryPageEncounterType(), BaseObsCohortDefinition.TimeModifier.ANY);
        CohortDefinition patientsWithTPTStartDate =  df.getPatientsWhoseObs(hivMetadata.getConcept("483939c7-79ba-4ca4-8c3e-346488c97fc7"),hivMetadata.getARTSummaryPageEncounterType());
        CohortDefinition patientsWithTPTEndDate =  df.getPatientsWhoseObs(hivMetadata.getConcept("813e21e7-4ccb-4fe9-aaab-3c0e40b6e356"),hivMetadata.getARTSummaryPageEncounterType());
        CohortDefinition patientsWithEitherTPTStartDateOrTPTEndDate = df.getPatientsInAny(patientsWithTPTStartDate,patientsWithTPTEndDate);
        CohortDefinition patientsEverOnART = df.getAnyEncounterOfTypesByEndOfDate(hivMetadata.getARTSummaryPageEncounterType());
        CohortDefinition patientsWithOutBothTPTStartAndEndDates = df.getPatientsNotIn(patientsEverOnART,patientsWithEitherTPTStartDateOrTPTEndDate);

        CohortDefinition eligbleForTPT = df.getPatientsInAny(startedTPTDuringQuarter,patientsWithOutBothTPTStartAndEndDates);

        CohortDefinition newOnARTEligbleForTPT = df.getPatientsInAll(clientsStartedOnARTAtThisFacilityDuringPeriod,noSignsOfTBDuringPeriod,eligbleForTPT);
        CohortDefinition alreadyOnARTEligbleForTPT = df.getPatientsInAll(clientsStartedOnARTAtThisFacilityBeforePeriod,noSignsOfTBDuringPeriod,eligbleForTPT);

        CohortDefinition bacteriallyConfirmedInPreviousQuarter = df.getPatientsWithCodedObsDuringPeriod(tbMetadata.getPatientType(),tbMetadata.getTBEnrollmentEncounterType(),Arrays.asList(tbMetadata.getBacteriologicallyConfirmed()),"3m", BaseObsCohortDefinition.TimeModifier.ANY);
        CohortDefinition registeredInPreviousQuarter = tbCohortDefinitionLibrary.getEnrolledOnDSTBDuringPeriod("3m");
        CohortDefinition registeredAndBacteriallyConfirmedinPreviousQuarter = df.getPatientsInAll(bacteriallyConfirmedInPreviousQuarter,registeredInPreviousQuarter);
        CohortDefinition patientsWithSmeartestDoneAfterIntensivePahse = df.getPatientsWithCodedObsDuringPeriod(hivMetadata.getConcept("dce0532c-30ab-102d-86b0-7a5022ba4115"),tbMetadata.getEncounterTypeList("455bad1f-5e97-4ee9-9558-ff1df8808732"), BaseObsCohortDefinition.TimeModifier.ANY);
        CohortDefinition patientsWithWithExaminationDateOFSmeartestDoneAfterIntensivePahse = df.getPatientsWhoseObsValueDateIsBetweenStartDateAndEndDate(hivMetadata.getConcept("d2f31713-aada-4d0d-9340-014b2371bdd8"),tbMetadata.getEncounterTypeList("455bad1f-5e97-4ee9-9558-ff1df8808732"), BaseObsCohortDefinition.TimeModifier.ANY);

        CohortDefinition registeredInPreviousAndSmearDoneAfter2MonthsIntensiveTreatment = df.getPatientsInAll(registeredAndBacteriallyConfirmedinPreviousQuarter,patientsWithSmeartestDoneAfterIntensivePahse,patientsWithWithExaminationDateOFSmeartestDoneAfterIntensivePahse);

        CohortDefinition positivesmearResults  =df.getPatientsWithCodedObsDuringPeriod(hivMetadata.getConcept("dce0532c-30ab-102d-86b0-7a5022ba4115"),tbMetadata.getEncounterTypeList("455bad1f-5e97-4ee9-9558-ff1df8808732"),Arrays.asList(tbMetadata.getConcept("dcdab74d-30ab-102d-86b0-7a5022ba4115"),tbMetadata.getConcept("dcdabb4b-30ab-102d-86b0-7a5022ba4115"),tbMetadata.getConcept("dcdabf68-30ab-102d-86b0-7a5022ba4115")), BaseObsCohortDefinition.TimeModifier.ANY);
        CohortDefinition negativesmearResults  =df.getPatientsWithCodedObsDuringPeriod(hivMetadata.getConcept("dce0532c-30ab-102d-86b0-7a5022ba4115"),tbMetadata.getEncounterTypeList("455bad1f-5e97-4ee9-9558-ff1df8808732"),Arrays.asList(tbMetadata.getConcept("dcdab32a-30ab-102d-86b0-7a5022ba4115")), BaseObsCohortDefinition.TimeModifier.ANY);

        add1AIndicators(dsd,"a","bacteria and registered ",bacteriologicallyConfirmedAndRegistered);
            add1AIndicators(dsd,"b","bacteria and on treatment",bacteriologicallyConfirmedAndStartedOnTratment);
            add1AIndicators(dsd,"c","clinicallyConfirmedAndRegistered",clinicallyConfirmedAndRegistered);
            add1AIndicators(dsd,"d","clinicallyConfirmedAndStartedOnTratment",clinicallyConfirmedAndStartedOnTratment);
            add1AIndicators(dsd,"e","EPTBConfirmedAndRegistered",EPTBConfirmedAndRegistered);
            add1AIndicators(dsd,"f","EPTBConfirmedAndStartedOnTratment",EPTBConfirmedAndStartedOnTratment);
            add1AIndicators(dsd,"g","assigned treatment supporter", noOfPatientsWithTreatmentSupporters);

            add1AIndicators(dsd1,"g","bacteria and registered children",df.getPatientsInAll(bacteriologicallyConfirmedAndRegistered,below15Years));
            add1AIndicators(dsd1,"h","bacteria and on treatment children",df.getPatientsInAll(bacteriologicallyConfirmedAndStartedOnTratment,below15Years));
            add1AIndicators(dsd1,"i","clinicallyConfirmedAndRegistered children",df.getPatientsInAll(clinicallyConfirmedAndRegistered,below15Years));
            add1AIndicators(dsd1,"j","clinicallyConfirmedAndStartedOnTratment children",df.getPatientsInAll(clinicallyConfirmedAndStartedOnTratment,below15Years));
            add1AIndicators(dsd1,"k","EPTBConfirmedAndRegistered children",df.getPatientsInAll(EPTBConfirmedAndRegistered,below15Years));
            add1AIndicators(dsd1,"l","EPTBConfirmedAndStartedOnTratment children",df.getPatientsInAll(EPTBConfirmedAndStartedOnTratment,below15Years));


            addGender(dsd2,"n","new relapsed and enrolled females",newAndRelapsedRegisteredClients);
            addGender(dsd2,"m","new relapsed and enrolled males ",newAndRelapsedRegisteredClients);

            splitGenderKeyAssigning(dsd3,"a","Total tested for HIV and with documented HIV Status ",newAndRelapsedPatientsWhoHaveHIVStatusNewlyDocumented);
            splitGenderKeyAssigning(dsd3,"b","newly Diagnosed HIV Positive ",df.getPatientsInAll(newAndRelapsedPatientsWhoHaveHIVStatusNewlyDocumented,newlyDiagnosedHIVPositive));
            splitGenderKeyAssigning(dsd3,"c","initiated On CPT During Period",df.getPatientsInAll(newAndRelapsedPatientsWhoHaveHIVStatusNewlyDocumented,newlyDiagnosedHIVPositive,initiatedOnCPTDuringPeriod));
            splitGenderKeyAssigning(dsd3,"d","initiated On ART During Period",df.getPatientsInAll(newAndRelapsedPatientsWhoHaveHIVStatusNewlyDocumented,newlyDiagnosedHIVPositive,initiatedOnARTDuringPeriod));
            splitGenderKeyAssigning(dsd3,"e","total known HIV+",df.getPatientsInAll(newAndRelapsedPatientsWhoHaveKnownHIVPositive));
            splitGenderKeyAssigning(dsd3,"f","total known HIV+ on CPT",df.getPatientsInAll(newAndRelapsedPatientsWhoHaveKnownHIVPositive,tbCohortDefinitionLibrary.getPatientsOnCPT()));
            splitGenderKeyAssigning(dsd3,"g","total known HIV+ on ART",df.getPatientsInAll(newAndRelapsedPatientsWhoHaveKnownHIVPositive,tbCohortDefinitionLibrary.getPatientsWhoAreAlreadyOnART()));

            addIndicator(dsd4,"FDOT","FDOT",df.getPatientsInAll(tbCohortDefinitionLibrary.getPatientsOnFacilityDOTSTreatmentModel(),newAndRelapsedPatients),"");
            addIndicator(dsd4,"CDOT","CDOT",df.getPatientsInAll(tbCohortDefinitionLibrary.getPatientsOnCommunityDOTSTreatmentModel(),newAndRelapsedPatients),"");

            addIndicator(dsd5,"healthWorkers","health workers",df.getPatientsInAll(newAndRelapsedPatients,patientsWhoAreHealthWorkers),"");
            addIndicator(dsd5,"TBContacts","contacts",df.getPatientsInAll(newAndRelapsedPatients,patientsWhoAreTBContacts),"");
            addIndicator(dsd5,"Refugees","Refugees",df.getPatientsInAll(newAndRelapsedPatients,patientsWhoAreRefugees),"");
            addIndicator(dsd5,"prisoners","prisoners",df.getPatientsInAll(newAndRelapsedPatients,patientsWhoArePrisoners),"");
            addIndicator(dsd5,"uniformed","uniformed",df.getPatientsInAll(newAndRelapsedPatients,patientsWhoAreUniformedPeople),"");
            addIndicator(dsd5,"fisher","fisher folks",df.getPatientsInAll(newAndRelapsedPatients,patientsWhoAreFisherMen),"");
            addIndicator(dsd5,"diabetic","diabetic",df.getPatientsInAll(newAndRelapsedPatients,patientsWhoAreDiabetic),"");
            addIndicator(dsd5,"miner","miner",df.getPatientsInAll(newAndRelapsedPatients,patientsWhoAreMiners),"");
            addIndicator(dsd5,"smoker","smoker",df.getPatientsInAll(newAndRelapsedPatients,patientsWhoAreSmokers),"");
            addIndicator(dsd5,"mental","mental",df.getPatientsInAll(newAndRelapsedPatients,patientsWhoAreMentallyIll),"");

            addIndicator(dsd6,"newMale1","newOnARTMale1",df.getPatientsInAll(newOnARTEligbleForTPT,males,cohortDefinitionLibrary.between0And4years()),"");
            addIndicator(dsd6,"newFemale1","newOnARTFemale1",df.getPatientsInAll(newOnARTEligbleForTPT,females,cohortDefinitionLibrary.between0And4years()),"");
            addIndicator(dsd6,"newMale2","newOnARTMale2",df.getPatientsInAll(newOnARTEligbleForTPT,males,cohortDefinitionLibrary.agedAtLeast(5)),"");
            addIndicator(dsd6,"newFemale2","newOnARTFemale2",df.getPatientsInAll(newOnARTEligbleForTPT,females,cohortDefinitionLibrary.agedAtLeast(5)),"");
            addIndicator(dsd6,"alreadyMale1","alreadyOnARTMale1",df.getPatientsInAll(alreadyOnARTEligbleForTPT,males,cohortDefinitionLibrary.between0And4years()),"");
            addIndicator(dsd6,"alreadyFemale1","alreadyOnARTFemale1",df.getPatientsInAll(alreadyOnARTEligbleForTPT,females,cohortDefinitionLibrary.between0And4years()),"");
            addIndicator(dsd6,"alreadyMale2","alreadyOnARTMale2",df.getPatientsInAll(alreadyOnARTEligbleForTPT,males,cohortDefinitionLibrary.agedAtLeast(5)),"");
            addIndicator(dsd6,"alreadyFemale2","alreadyOnARTFemale2",df.getPatientsInAll(alreadyOnARTEligbleForTPT,females,cohortDefinitionLibrary.agedAtLeast(5)),"");

            addIndicator(dsd7, "A","all registered with PTB previous quarter ",registeredAndBacteriallyConfirmedinPreviousQuarter,"");
            addIndicator(dsd7, "B","all registered with PTB previous quarter that had smear test done ",registeredInPreviousAndSmearDoneAfter2MonthsIntensiveTreatment,"");
            addIndicator(dsd7, "C"," had smear test done with negative results ",df.getPatientsInAll(registeredInPreviousAndSmearDoneAfter2MonthsIntensiveTreatment,negativesmearResults),"");
            addIndicator(dsd7, "D"," had smear test done with postive results ",df.getPatientsInAll(registeredInPreviousAndSmearDoneAfter2MonthsIntensiveTreatment,positivesmearResults),"");
        return rd;
    }



    public void add1AIndicators(CohortIndicatorDataSetDefinition dsd, String key, String label, CohortDefinition cohortDefinition) {
        addIndicator(dsd, "1"+key , label + " new  ", cohortDefinition, "type=newPatients");
        addIndicator(dsd, "2"+key , label + " relapsed ", cohortDefinition, "type=relapsedPatients");
        addIndicator(dsd, "3"+key , label + " treatedAfterLTFP", cohortDefinition, "type=treatedAfterLTFP");
        addIndicator(dsd, "4"+key , label + " treatedAfterFailure ", cohortDefinition, "type=treatedAfterFailure");
        addIndicator(dsd, "5"+key , label + " treatementHistoryUnknown", cohortDefinition, "type=treatementHistoryUnknown");
        addIndicator(dsd, "6"+key , label + " overall", cohortDefinition, "");
        addIndicator(dsd, "7"+key , label + " Referred from Community activities", cohortDefinition, "type=referred");
    }

    public void addIndicator(CohortIndicatorDataSetDefinition dsd, String key, String label, CohortDefinition cohortDefinition, String dimensionOptions) {
        CohortIndicator ci = new CohortIndicator();
        ci.addParameter(ReportingConstants.START_DATE_PARAMETER);
        ci.addParameter(ReportingConstants.END_DATE_PARAMETER);
        ci.setType(CohortIndicator.IndicatorType.COUNT);
        ci.setCohortDefinition(Mapped.mapStraightThrough(cohortDefinition));
        dsd.addColumn(key, label, Mapped.mapStraightThrough(ci), dimensionOptions);
    }



    public void addGender(CohortIndicatorDataSetDefinition dsd, String key, String label, CohortDefinition cohortDefinition) {
        if (key == "n") {
            addIndicator(dsd, "2n", label, cohortDefinition, "age=below1female");
            addIndicator(dsd, "3n", label, cohortDefinition, "age=between1and4female");
            addIndicator(dsd, "4n", label, cohortDefinition, "age=between5and9female");
            addIndicator(dsd, "5n", label, cohortDefinition, "age=between10and14female");
            addIndicator(dsd, "6n", label, cohortDefinition, "age=between15and19female");
            addIndicator(dsd, "7n", label, cohortDefinition, "age=between20and24female");
            addIndicator(dsd, "8n", label, cohortDefinition, "age=between25and29female");
            addIndicator(dsd, "9n", label, cohortDefinition, "age=between30and34female");
            addIndicator(dsd, "10n", label, cohortDefinition, "age=between35and39female");
            addIndicator(dsd, "11n", label, cohortDefinition, "age=between40and44female");
            addIndicator(dsd, "12n", label, cohortDefinition, "age=between45and49female");
            addIndicator(dsd, "13n", label, cohortDefinition, "age=above50female");
        } else if (key == "m") {
            addIndicator(dsd, "2m", label, cohortDefinition, "age=below1male");
            addIndicator(dsd, "3m", label, cohortDefinition, "age=between1and4male");
            addIndicator(dsd, "4m", label, cohortDefinition, "age=between5and9male");
            addIndicator(dsd, "5m", label, cohortDefinition, "age=between10and14male");
            addIndicator(dsd, "6m", label, cohortDefinition, "age=between15and19male");
            addIndicator(dsd, "7m", label, cohortDefinition, "age=between20and24male");
            addIndicator(dsd, "8m", label, cohortDefinition, "age=between25and29male");
            addIndicator(dsd, "9m", label, cohortDefinition, "age=between30and34male");
            addIndicator(dsd, "10m", label, cohortDefinition, "age=between35and39male");
            addIndicator(dsd, "11m", label, cohortDefinition, "age=between40and44male");
            addIndicator(dsd, "12m", label, cohortDefinition, "age=between45and49male");
            addIndicator(dsd, "13m", label, cohortDefinition, "age=above50male");
        }
    }

    public void addOtherDimensionWithKey(CohortIndicatorDataSetDefinition dsd,String dimensionKey, String key, String label, CohortDefinition cohortDefinition) {
        if (key == "n") {
            addIndicator(dsd, dimensionKey+"2n", label, cohortDefinition, "age=below1female");
            addIndicator(dsd, dimensionKey+"3n", label, cohortDefinition, "age=between1and4female");
            addIndicator(dsd, dimensionKey+"4n", label, cohortDefinition, "age=between5and9female");
            addIndicator(dsd, dimensionKey+"5n", label, cohortDefinition, "age=between10and14female");
            addIndicator(dsd, dimensionKey+"6n", label, cohortDefinition, "age=between15and19female");
            addIndicator(dsd, dimensionKey+"7n", label, cohortDefinition, "age=between20and24female");
            addIndicator(dsd, dimensionKey+"8n", label, cohortDefinition, "age=between25and29female");
            addIndicator(dsd, dimensionKey+"9n", label, cohortDefinition, "age=between30and34female");
            addIndicator(dsd, dimensionKey+"10n", label, cohortDefinition, "age=between35and39female");
            addIndicator(dsd, dimensionKey+"11n", label, cohortDefinition, "age=between40and44female");
            addIndicator(dsd, dimensionKey+"12n", label, cohortDefinition, "age=between45and49female");
            addIndicator(dsd, dimensionKey+"13n", label, cohortDefinition, "age=above50female");
        } else if (key == "m") {
            addIndicator(dsd, dimensionKey+"2m", label, cohortDefinition, "age=below1male");
            addIndicator(dsd, dimensionKey+"3m", label, cohortDefinition, "age=between1and4male");
            addIndicator(dsd, dimensionKey+"4m", label, cohortDefinition, "age=between5and9male");
            addIndicator(dsd, dimensionKey+"5m", label, cohortDefinition, "age=between10and14male");
            addIndicator(dsd, dimensionKey+"6m", label, cohortDefinition, "age=between15and19male");
            addIndicator(dsd, dimensionKey+"7m", label, cohortDefinition, "age=between20and24male");
            addIndicator(dsd, dimensionKey+"8m", label, cohortDefinition, "age=between25and29male");
            addIndicator(dsd, dimensionKey+"9m", label, cohortDefinition, "age=between30and34male");
            addIndicator(dsd, dimensionKey+"10m", label, cohortDefinition, "age=between35and39male");
            addIndicator(dsd, dimensionKey+"11m", label, cohortDefinition, "age=between40and44male");
            addIndicator(dsd, dimensionKey+"12m", label, cohortDefinition, "age=between45and49male");
            addIndicator(dsd, dimensionKey+"13m", label, cohortDefinition, "age=above50male");
        }
    }

    public void splitGenderKeyAssigning(CohortIndicatorDataSetDefinition dsd,String dimensionKey,String label,CohortDefinition cohortDefinition){
        addOtherDimensionWithKey(dsd,dimensionKey,"n",label+" females",cohortDefinition);
        addOtherDimensionWithKey(dsd,dimensionKey,"m",label+" males",cohortDefinition);

    }

        @Override
    public String getVersion() {
        return "1.0.9";
    }
}