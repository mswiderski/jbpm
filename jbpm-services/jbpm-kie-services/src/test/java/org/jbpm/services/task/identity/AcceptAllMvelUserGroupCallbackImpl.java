package org.jbpm.services.task.identity;

import org.jbpm.services.task.identity.MvelUserGroupCallbackImpl;


public class AcceptAllMvelUserGroupCallbackImpl extends MvelUserGroupCallbackImpl {

    public AcceptAllMvelUserGroupCallbackImpl() {
        super(true);
    }

    @Override
    public boolean existsUser(String userId) {
        return true;
    }

    @Override
    public boolean existsGroup(String groupId) {
        return true;
    }

}
