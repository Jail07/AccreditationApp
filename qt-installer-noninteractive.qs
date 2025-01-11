var installer = gui.createInstaller();
installer.setMessageBoxAutomaticAnswer("installationErrorWithRetry", "Retry");
installer.setMessageBoxAutomaticAnswer("error", "Abort");

var widget = installer.addWizardPage("WelcomePage");
widget.enterPage.connect(function() {
    widget.pageById("IntroductionPage").nextButton.click();
    widget.pageById("TargetDirectoryPage").nextButton.click();
    widget.pageById("ComponentSelectionPage").nextButton.click();
    widget.pageById("StartInstallationPage").nextButton.click();
});
