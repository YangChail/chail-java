package org.chail.common.krb;

import javax.security.auth.Subject;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.login.LoginException;
import javax.security.auth.spi.LoginModule;
import java.util.Map;

public class MyHadoopLoginModule implements LoginModule {

    /**
     * Initialize this LoginModule.
     *
     * <p> This method is called by the {@code LoginContext}
     * after this {@code LoginModule} has been instantiated.
     * The purpose of this method is to initialize this
     * {@code LoginModule} with the relevant information.
     * If this {@code LoginModule} does not understand
     * any of the data stored in {@code sharedState} or
     * {@code options} parameters, they can be ignored.
     *
     * <p>
     *
     * @param subject         the {@code Subject} to be authenticated. <p>
     * @param callbackHandler a {@code CallbackHandler} for communicating
     *                        with the end user (prompting for usernames and
     *                        passwords, for example). <p>
     * @param sharedState     state shared with other configured LoginModules. <p>
     * @param options         options specified in the login
     *                        {@code Configuration} for this particular
     *                        {@code LoginModule}.
     */
    @Override
    public void initialize(Subject subject, CallbackHandler callbackHandler, Map<String, ?> sharedState, Map<String, ?> options) {
        System.out.println("登陆");
    }

    /**
     * Method to authenticate a {@code Subject} (phase 1).
     *
     * <p> The implementation of this method authenticates
     * a {@code Subject}.  For example, it may prompt for
     * {@code Subject} information such
     * as a username and password and then attempt to verify the password.
     * This method saves the result of the authentication attempt
     * as private state within the LoginModule.
     *
     * <p>
     *
     * @return true if the authentication succeeded, or false if this
     * {@code LoginModule} should be ignored.
     * @throws LoginException if the authentication fails
     */
    @Override
    public boolean login() throws LoginException {
        return false;
    }

    /**
     * Method to commit the authentication process (phase 2).
     *
     * <p> This method is called if the LoginContext's
     * overall authentication succeeded
     * (the relevant REQUIRED, REQUISITE, SUFFICIENT and OPTIONAL LoginModules
     * succeeded).
     *
     * <p> If this LoginModule's own authentication attempt
     * succeeded (checked by retrieving the private state saved by the
     * {@code login} method), then this method associates relevant
     * Principals and Credentials with the {@code Subject} located in the
     * {@code LoginModule}.  If this LoginModule's own
     * authentication attempted failed, then this method removes/destroys
     * any state that was originally saved.
     *
     * <p>
     *
     * @return true if this method succeeded, or false if this
     * {@code LoginModule} should be ignored.
     * @throws LoginException if the commit fails
     */
    @Override
    public boolean commit() throws LoginException {
        return false;
    }

    /**
     * Method to abort the authentication process (phase 2).
     *
     * <p> This method is called if the LoginContext's
     * overall authentication failed.
     * (the relevant REQUIRED, REQUISITE, SUFFICIENT and OPTIONAL LoginModules
     * did not succeed).
     *
     * <p> If this LoginModule's own authentication attempt
     * succeeded (checked by retrieving the private state saved by the
     * {@code login} method), then this method cleans up any state
     * that was originally saved.
     *
     * <p>
     *
     * @return true if this method succeeded, or false if this
     * {@code LoginModule} should be ignored.
     * @throws LoginException if the abort fails
     */
    @Override
    public boolean abort() throws LoginException {
        System.out.println("登陆失败");
        return false;
    }

    /**
     * Method which logs out a {@code Subject}.
     *
     * <p>An implementation of this method might remove/destroy a Subject's
     * Principals and Credentials.
     *
     * <p>
     *
     * @return true if this method succeeded, or false if this
     * {@code LoginModule} should be ignored.
     * @throws LoginException if the logout fails
     */
    @Override
    public boolean logout() throws LoginException {
        return false;
    }
}
