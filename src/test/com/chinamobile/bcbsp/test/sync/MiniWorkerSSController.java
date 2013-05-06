/**
 * CopyRight by Chinamobile
 */
package com.chinamobile.bcbsp.test.sync;

import com.chinamobile.bcbsp.sync.SuperStepReportContainer;
import com.chinamobile.bcbsp.sync.WorkerSSControllerInterface;

public class MiniWorkerSSController implements WorkerSSControllerInterface {

    @Override
    public boolean checkPointStageSuperStepBarrier(int superStepCounter,
            SuperStepReportContainer ssrc) {
        System.out.println("checkPointStageSuperStepBarrier is invoked!");
        return true;
    }

    @Override
    public boolean firstStageSuperStepBarrier(int superStepCounter,
            SuperStepReportContainer ssrc) {
        System.out.println("FirstStageSuperStepBarrier is invoked!");
        return true;
    }

    @Override
    public boolean saveResultStageSuperStepBarrier(int superStepCounter,
            SuperStepReportContainer ssrc) {
        System.out.println("saveResultStageSuperStepBarrier is invoked!");
        return true;
    }

    @Override
    public boolean secondStageSuperStepBarrier(int superStepCounter,
            SuperStepReportContainer ssrc) {
        System.out.println("SecondStageSuperStepBarrier is invoked!");
        return true;
    }
}
