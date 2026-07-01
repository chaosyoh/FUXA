import { Component, ElementRef, Inject, ViewChild, AfterViewInit, OnDestroy, NgZone } from '@angular/core';
import { UntypedFormControl } from '@angular/forms';
import { MatDialogRef as MatDialogRef, MAT_DIALOG_DATA as MAT_DIALOG_DATA } from '@angular/material/dialog';
import { Subscription } from 'rxjs';

import { AuthService } from '../_services/auth.service';
import { ProjectService } from '../_services/project.service';
import { TranslateService } from '@ngx-translate/core';
import { NgxTouchKeyboardDirective } from '../framework/ngx-touch-keyboard/ngx-touch-keyboard.directive';
import { LoginOverlayColorType } from '../_models/hmi';

@Component({
	selector: 'app-login',
	templateUrl: './login.component.html',
	styleUrls: ['./login.component.scss']
})
export class LoginComponent implements AfterViewInit, OnDestroy {

    @ViewChild('touchKeyboard', {static: false}) touchKeyboard: NgxTouchKeyboardDirective;

	loading = false;
	showPassword = false;
	submitLoading = false;
	messageError: string;
	username: UntypedFormControl = new UntypedFormControl();
	password: UntypedFormControl = new UntypedFormControl();
	errorEnabled = false;
	disableCancel = false;
	isValidForm = false; // Track form validity for button state
	private subscriptions = new Subscription();
	private autofillCheckTimer: any;

	constructor(private authService: AuthService,
				private projectService: ProjectService,
				private translateService: TranslateService,
				private dialogRef: MatDialogRef<LoginComponent>,
				private ngZone: NgZone,
				@Inject(MAT_DIALOG_DATA) private data: any) {
		const hmi = this.projectService.getHmi();
		this.disableCancel = hmi?.layout?.loginonstart && hmi.layout?.loginoverlaycolor !== LoginOverlayColorType.none;
		
		// Subscribe to form control changes to update validity state
		this.subscriptions.add(this.username.valueChanges.subscribe(() => this.updateFormValidity()));
		this.subscriptions.add(this.password.valueChanges.subscribe(() => this.updateFormValidity()));
	}

	ngAfterViewInit() {
		// Detect browser autofill using :-webkit-autofill CSS selector.
		// This works even when the browser doesn't expose input.value for security reasons.
		this.startAutofillDetection();
	}

	/**
	 * Periodically check if inputs are autofilled using :-webkit-autofill selector.
	 * Stops after both are detected or after 5 seconds.
	 */
	startAutofillDetection() {
		const maxDuration = 5000; // 5 seconds max
		const interval = 150;
		const startTime = Date.now();

		const check = () => {
			const usernameAutofilled = document.querySelector('input[autocomplete="username"]:-webkit-autofill');
			const passwordAutofilled = document.querySelector('input[autocomplete="current-password"]:-webkit-autofill');

			if (usernameAutofilled && passwordAutofilled) {
				// Both inputs are autofilled - enable the button
				this.ngZone.run(() => {
					this.isValidForm = true;
				});
				return; // Stop checking
			}

			// Also try to sync any readable values
			this.checkAutofillAndSync();

			if (Date.now() - startTime < maxDuration) {
				this.autofillCheckTimer = setTimeout(check, interval);
			}
		};

		this.autofillCheckTimer = setTimeout(check, interval);
	}

	onNoClick(): void {
		this.dialogRef.close();
	}

	onOkClick(): void {
		this.errorEnabled = true;
		this.messageError = '';
		// Sync FormControl values with DOM in case of browser autofill
		this.checkAutofillAndSync();
		// If FormControl still has no value, read directly from DOM
		const usernameEl = document.querySelector('input[autocomplete="username"]') as HTMLInputElement;
		const passwordEl = document.querySelector('input[autocomplete="current-password"]') as HTMLInputElement;
		if (!this.username.value && usernameEl?.value) {
			this.username.setValue(usernameEl.value);
		}
		if (!this.password.value && passwordEl?.value) {
			this.password.setValue(passwordEl.value);
		}
		this.signIn();
	}

	/**
	 * Check if browser has autofilled values and sync them to FormControl
	 */
	checkAutofillAndSync() {
		const usernameEl = document.querySelector('input[autocomplete="username"]') as HTMLInputElement;
		const passwordEl = document.querySelector('input[autocomplete="current-password"]') as HTMLInputElement;
		
		let updated = false;
		if (usernameEl?.value && !this.username.value) {
			this.username.setValue(usernameEl.value);
			updated = true;
		}
		if (passwordEl?.value && !this.password.value) {
			this.password.setValue(passwordEl.value);
			updated = true;
		}
		
		if (updated) {
			this.updateFormValidity();
		}
	}

	/**
	 * Called on input event to handle browser autofill detection
	 */
	onInputChange() {
		// Force Angular to detect changes when browser autofills
		setTimeout(() => {
			this.checkAutofillAndSync();
		}, 0);
	}

	/**
	 * Update the form validity state based on current values
	 */
	updateFormValidity() {
		// Check both FormControl values and DOM values for autofill cases
		const usernameValue = this.username.value || 
			(document.querySelector('input[autocomplete="username"]') as HTMLInputElement)?.value;
		const passwordValue = this.password.value || 
			(document.querySelector('input[autocomplete="current-password"]') as HTMLInputElement)?.value;
		
		this.isValidForm = !!(usernameValue && passwordValue);
	}

	isValidate(usernameEl?: HTMLInputElement, passwordEl?: HTMLInputElement) {
		const usernameValue = this.username.value || usernameEl?.value;
		const passwordValue = this.password.value || passwordEl?.value;
		if (usernameValue && passwordValue) {
			return true;
		}
		return false;
	}

	signIn() {
		this.submitLoading = true;
		this.authService.signIn(this.username.value, this.password.value).subscribe(result => {
			this.submitLoading = false;
			this.dialogRef.close(true);
			this.projectService.reload();
		}, error => {
			this.submitLoading = false;
			console.log(error)
			if(typeof error == 'string'){
				this.messageError = error;
			}
			else{
				this.translateService.get('msg.signin-failed').subscribe((txt: string) => this.messageError = txt);
			}
		});
	}

    keyDownStopPropagation(event) {
        event.stopPropagation();
    }

	onFocus(event: FocusEvent) {
		const hmi = this.projectService.getHmi();
		if (hmi?.layout?.inputdialog?.includes('keyboard')) {
			if (hmi.layout.inputdialog === 'keyboardFullScreen') {
				this.touchKeyboard.ngxTouchKeyboardFullScreen = true;
			}
			this.touchKeyboard.closePanel();
			const targetElement = event.target as HTMLInputElement;
			const elementRef = new ElementRef<HTMLInputElement>(targetElement);
			this.touchKeyboard.openPanel(elementRef);
		}
    }

	ngOnDestroy() {
		this.subscriptions.unsubscribe();
		if (this.autofillCheckTimer) {
			clearTimeout(this.autofillCheckTimer);
		}
	}
}
