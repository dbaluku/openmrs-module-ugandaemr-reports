package org.openmrs.module.ugandaemrreports.reports;

import org.openmrs.module.reporting.data.patient.library.BuiltInPatientDataLibrary;
import org.openmrs.module.reporting.evaluation.parameter.Mapped;
import org.openmrs.module.reporting.evaluation.parameter.Parameter;
import org.openmrs.module.reporting.report.ReportDesign;
import org.openmrs.module.reporting.report.definition.ReportDefinition;
import org.openmrs.module.ugandaemrreports.definition.dataset.definition.ARTDatasetDefinition2019;
import org.openmrs.module.ugandaemrreports.library.*;
import org.openmrs.module.ugandaemrreports.metadata.CommonReportMetadata;
import org.openmrs.module.ugandaemrreports.metadata.HIVMetadata;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Daily Appointments List report
 */
@Component
public class SetupARTRegister2019 extends UgandaEMRDataExportManager {

    @Autowired
    private DataFactory df;

    @Autowired
    ARTClinicCohortDefinitionLibrary hivCohorts;

    @Autowired
    private CommonReportMetadata commonMetadata;

    @Autowired
    private HIVCohortDefinitionLibrary hivCohortDefinitionLibrary;

    @Autowired
    private HIVMetadata hivMetadata;

    @Autowired
    private BuiltInPatientDataLibrary builtInPatientData;

    @Autowired
    private HIVPatientDataLibrary hivPatientData;

    @Autowired
    private BasePatientDataLibrary basePatientData;

    /**
     * @return the uuid for the report design for exporting to Excel
     */
    @Override
    public String getExcelDesignUuid() {
        return "c62ac620-7356-4d09-ac28-01b79ba37025";
    }

    @Override
    public String getUuid() {
        return "7561229b-a35b-4afd-82a5-83673d6f4ad6";
    }

    @Override
    public String getName() {
        return "ART Register 2019";
    }

    @Override
    public String getDescription() {
        return "ART register 2019 version";
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
        List<ReportDesign> l = new ArrayList<ReportDesign>();
        l.add(buildReportDesign(reportDefinition));
        l.add(buildExcel(reportDefinition));
        return l;
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
        ReportDesign rd = createCSVDesign(getExcelDesignUuid(), reportDefinition);
        return rd;
    }
    public ReportDesign buildExcel(ReportDefinition reportDefinition) {
        ReportDesign rd = createExcelTemplateDesign("d29a2bbb-2915-4112-9bb9-1b7321723ca3", reportDefinition, "ARTRegister2019.xls");
        Properties props = new Properties();
        props.put("repeatingSections", "sheet:1,row:4-7,dataset:ART | sheet:2,row:4-9,dataset:ART | sheet:3,row:4-9,dataset:ART | sheet:4,row:4-9,dataset:ART | sheet:5,row:4-9,dataset:ART");
        props.put("sortWeight", "5000");
        rd.setProperties(props);
        return rd;
    }

    @Override
    public ReportDefinition constructReportDefinition() {

        ReportDefinition rd = new ReportDefinition();
        rd.setUuid(getUuid());
        rd.setName(getName());
        rd.setDescription(getDescription());
        rd.setParameters(getParameters());

        ARTDatasetDefinition2019 dsd = new ARTDatasetDefinition2019();
        dsd.setName(getName());
        dsd.setParameters(getParameters());
        rd.addDataSetDefinition("ART", Mapped.mapStraightThrough(dsd));
        return rd;
    }

    @Override
    public String getVersion() {
        return "0.0.4.5";
    }
}
