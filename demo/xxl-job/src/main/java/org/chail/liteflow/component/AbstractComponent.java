package org.chail.liteflow.component;

import com.yomahub.liteflow.core.NodeComponent;

public abstract class AbstractComponent extends NodeComponent {

    public abstract void process() throws Exception;
}
