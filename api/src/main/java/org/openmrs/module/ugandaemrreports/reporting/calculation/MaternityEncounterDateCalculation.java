package org.openmrs.module.ugandaemrreports.reporting.calculation;

import org.openmrs.Encounter;
import org.openmrs.EncounterType;
import org.openmrs.calculation.patient.PatientCalculationContext;
import org.openmrs.calculation.result.CalculationResultMap;
import org.openmrs.calculation.result.SimpleResult;
import org.openmrs.module.metadatadeploy.MetadataUtils;

import java.util.Collection;
import java.util.Date;
import java.util.Map;

public class MaternityEncounterDateCalculation  extends AbstractPatientCalculation{
    @Override
    public CalculationResultMap evaluate(Collection<Integer> cohort, Map<String, Object> map, PatientCalculationContext context) {
        CalculationResultMap ret = new CalculationResultMap();

        CalculationResultMap encounter = Calculations.lastEncounter(MetadataUtils.existing(EncounterType.class, "a9f11592-22e7-45fc-904d-dfe24cb1fc67"), cohort, context);
        for(Integer ptId: cohort){
            Date encounterDate = null;
            Encounter enc = EmrCalculationUtils.encounterResultForPatient(encounter, ptId);
            if(enc != null) {
                encounterDate = enc.getEncounterDatetime();
            }
            ret.put(ptId, new SimpleResult(encounterDate, this));
        }

        return ret;
    }

}
