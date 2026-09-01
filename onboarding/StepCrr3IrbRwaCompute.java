package com.bnpparibas.sit.credit.risk.steps.crr3.irb;

import com.bnpparibas.sit.credit.risk.core.model.Rating;
import com.bnpparibas.sit.credit.risk.model.Facility;
import com.bnpparibas.sit.credit.risk.model.MeasurementsOfRating;
import com.bnpparibas.sit.credit.risk.model.crr.Crr3Approach;
import com.bnpparibas.sit.credit.risk.steps.AbstractStepMap;
import com.bnpparibas.sit.credit.risk.utils.ConfigurationConstants;
import com.bnpparibas.sit.credit.risk.utils.StepConstants;
import org.apache.commons.math3.util.FastMath;
import org.apache.spark.SparkException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.beans.factory.config.PlaceholderConfigurerSupport;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component(StepConstants.CRR3_IRB_COMPUTE_RWA)
@Scope(value = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class StepCrr3IrbRwaCompute extends AbstractStepMap {

    @Value(PlaceholderConfigurerSupport.DEFAULT_PLACEHOLDER_PREFIX + ConfigurationConstants.PROPERTY_USE_CRR3_OPTIONS +
            PlaceholderConfigurerSupport.DEFAULT_PLACEHOLDER_SUFFIX)
    private boolean useCrr3;

    public static double RATIO_CAPITAL_TO_RWA = 12.5;

    @Override
    public Facility execute(Facility facility) throws SparkException {
        //if(!useCrr3) return facility;

        if (facility.getApproche_bale_iv_rwa() != null && facility.getApproche_bale_iv_rwa() != Crr3Approach.IRBA
                && facility.getApproche_bale_iv_rwa() != Crr3Approach.IRBF) {
            return facility;
        }
        if(!facility.getfMeasurement().getMeasurementsByRating().values().stream()
                .anyMatch( measurementsOfRating->(measurementsOfRating.getUnpaid()!=null && measurementsOfRating.getUnpaid().isClean()))){
            return facility;
        }

        double ead = 0;
        for (MeasurementsOfRating value : facility.getfMeasurement().getMeasurementsByRating().values()) {
            if (value.isDirty()) continue;
            ead += value.getEadReg();
        }

        double unsecured_ead_ratio;
        double secured_ead_ratio;
        double secured_rw;
        if (facility.getApproche_bale_iv_rwa() == Crr3Approach.IRBA) {
            unsecured_ead_ratio = facility.getCrr3_irba_per_unsecured_ead();
            secured_ead_ratio = facility.getCcr3_irba_secured_ead_ratio();
            secured_rw = facility.getCcr3_irba_secured_rw();
        } else {
            unsecured_ead_ratio = facility.getCrr3_irbf_per_unsecured_ead();
            secured_ead_ratio = facility.getCcr3_irbf_secured_ead_ratio();
            secured_rw = facility.getCcr3_irbf_secured_rw();
        }

        /* unsecured */
        for(Map.Entry<Rating, MeasurementsOfRating> mapEntry : facility.getfMeasurement().getMeasurementsByRating().entrySet()) {
            MeasurementsOfRating measurementsOfRating = mapEntry.getValue();

            if (measurementsOfRating.isDirty()) {
                measurementsOfRating.setCrr3_irb_unsecured_rwa(0.0);
                continue;
            }

            if (unsecured_ead_ratio == 0.0 || measurementsOfRating.getEadReg() == 0) {
                measurementsOfRating.setCrr3_irb_unsecured_rwa(0.0);
            } else {
                double rwa_unsecured = measurementsOfRating.getCrr3_irb_partial_unsecured_capital() * RATIO_CAPITAL_TO_RWA;
                measurementsOfRating.setCrr3_irb_unsecured_rwa(rwa_unsecured);

                double rw_unsecured = rwa_unsecured/(measurementsOfRating.getEadReg() * unsecured_ead_ratio);
                measurementsOfRating.setCrr3_irb_unsecured_rw(rw_unsecured);
            }
            measurementsOfRating.setCrr3_irb_unsecured_capital(measurementsOfRating.getCrr3_irb_unsecured_rwa()/RATIO_CAPITAL_TO_RWA);
        }


        /* secured */
        for(Map.Entry<Rating, MeasurementsOfRating> mapEntry : facility.getfMeasurement().getMeasurementsByRating().entrySet()) {
            MeasurementsOfRating measurementsOfRating = mapEntry.getValue();

            if (measurementsOfRating.isDirty()) {
                measurementsOfRating.setCrr3_irb_secured_rwa(0.0);
                continue;
            }

            if (secured_ead_ratio == 0.0 || measurementsOfRating.getEadReg() == 0) {
                measurementsOfRating.setCrr3_irb_secured_rwa(0.0);
            } else {
                double rwa_tmp_secured = measurementsOfRating.getCrr3_irb_partial_secured_capital() * RATIO_CAPITAL_TO_RWA;

                double rw_tmp = rwa_tmp_secured/(measurementsOfRating.getEadReg() * secured_ead_ratio);

                double rw = FastMath.min(secured_rw, rw_tmp);
                double rwMax = FastMath.max(secured_rw, rw_tmp);
                measurementsOfRating.setCrr3_irb_secured_rw(rw);
                measurementsOfRating.setCrr3_irb_secured_rw_max(rwMax);

                double rwa_secured = measurementsOfRating.getEadReg() * rw * secured_ead_ratio;

                measurementsOfRating.setCrr3_irb_secured_rwa(rwa_secured);
            }
            measurementsOfRating.setCrr3_irb_secured_capital(measurementsOfRating.getCrr3_irb_secured_rwa()/RATIO_CAPITAL_TO_RWA);
        }

        /* total */
        for(Map.Entry<Rating, MeasurementsOfRating> mapEntry : facility.getfMeasurement().getMeasurementsByRating().entrySet()) {
            MeasurementsOfRating measurementsOfRating = mapEntry.getValue();

            measurementsOfRating.setCrr3_irb_rwa(measurementsOfRating.getCrr3_irb_secured_rwa() + measurementsOfRating.getCrr3_irb_unsecured_rwa());
        }

        /* agg */
        double rwa_unsecured = 0;
        double rwa_secured = 0;
        double rw_secured_min = 0;
        double rw_secured_max = 0;
        for(Map.Entry<Rating, MeasurementsOfRating> mapEntry : facility.getfMeasurement().getMeasurementsByRating().entrySet()) {
            MeasurementsOfRating measurementsOfRating = mapEntry.getValue();
            rwa_unsecured += measurementsOfRating.getCrr3_irb_unsecured_rwa();
            rwa_secured += measurementsOfRating.getCrr3_irb_secured_rwa();
            rw_secured_min += measurementsOfRating.getCrr3_irb_secured_rw();
            rw_secured_max += measurementsOfRating.getCrr3_irb_secured_rw_max();
        }
        facility.setCrr3_irb_secured_rwa(rwa_secured);
        facility.setCrr3_irb_unsecured_rwa(rwa_unsecured);
        facility.setCrr3_irb_rwa(rwa_secured+rwa_unsecured);
        facility.setCrr3_irb_secured_rw_min(rw_secured_min);
        facility.setCrr3_irb_secured_rw_max(rw_secured_max);

        facility.setCrr3_irb_secured_capital(rwa_secured/RATIO_CAPITAL_TO_RWA);
        facility.setCrr3_irb_unsecured_capital(rwa_unsecured/RATIO_CAPITAL_TO_RWA);
        facility.setCrr3_irb_capital(facility.getCrr3_irb_unsecured_capital()+facility.getCrr3_irb_secured_capital());

        return facility;
    }
}
