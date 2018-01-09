/*
 * vim:noexpandtab:shiftwidth=8:tabstop=8:
 *
 * Copyright (C) Cynny Space, 2018
 * Author: Aurélien Rainone  aurelien.rainone@gmail.com
 *
 * contributeur : Aurélien Rainone  aurelien.rainone@gmail.com
 *
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA
 *
 * -------------
 */

/* internal.c
 * KVSNS: S3 extstore internal definitions.
 */

#include "internal.h"


int s3status2posix_error(const S3Status s3_errorcode)
{
	int rc;

	switch (s3_errorcode) {

		case S3StatusOK:
			rc = 0;

		/*
		 * Errors that prevent the S3 request from being issued or
		 * response from being read
		 */
		case S3StatusInternalError:
			rc = ECONNABORTED;
			break;

		case S3StatusOutOfMemory:
			rc = ENOMEM;
			break;

		case S3StatusInterrupted:
			rc = ECONNABORTED;
			break;

		case S3StatusInvalidBucketNameTooLong:
		case S3StatusInvalidBucketNameFirstCharacter:
		case S3StatusInvalidBucketNameCharacter:
		case S3StatusInvalidBucketNameCharacterSequence:
		case S3StatusInvalidBucketNameTooShort:
		case S3StatusInvalidBucketNameDotQuadNotation:
		case S3StatusQueryParamsTooLong:
		case S3StatusFailedToInitializeRequest:
			rc = EINVAL;
			break;

		case S3StatusMetaDataHeadersTooLong:
		case S3StatusBadMetaData:
		case S3StatusBadContentType:
		case S3StatusContentTypeTooLong:
		case S3StatusBadMD5:
		case S3StatusMD5TooLong:
		case S3StatusBadCacheControl:
		case S3StatusCacheControlTooLong:
		case S3StatusBadContentDispositionFilename:
		case S3StatusContentDispositionFilenameTooLong:
		case S3StatusBadContentEncoding:
		case S3StatusContentEncodingTooLong:
		case S3StatusBadIfMatchETag:
		case S3StatusIfMatchETagTooLong:
		case S3StatusBadIfNotMatchETag:
		case S3StatusIfNotMatchETagTooLong:
		case S3StatusHeadersTooLong:
		case S3StatusKeyTooLong:
		case S3StatusUriTooLong:
		case S3StatusXmlParseFailure:
		case S3StatusEmailAddressTooLong:
		case S3StatusUserIdTooLong:
		case S3StatusUserDisplayNameTooLong:
		case S3StatusGroupUriTooLong:
		case S3StatusPermissionTooLong:
		case S3StatusTargetBucketTooLong:
		case S3StatusTargetPrefixTooLong:
			rc = EINVAL;
			break;

		case S3StatusTooManyGrants:
			rc = E2BIG;
			break;

		case S3StatusBadGrantee:
		case S3StatusBadPermission:
			rc = EINVAL;
			break;

		case S3StatusXmlDocumentTooLarge:
			rc = E2BIG;
			break;

		case S3StatusNameLookupError:
			rc = ENOENT;
			break;

		case S3StatusFailedToConnect:
			rc = EHOSTUNREACH;
			break;

		case S3StatusServerFailedVerification:
			rc = EACCES;
			break;

		case S3StatusConnectionFailed:
			rc = ENOTCONN;
			break;

		case S3StatusAbortedByCallback:
			rc = EINTR;
			break;

		case S3StatusNotSupported:
			rc = EPERM;
			break;

		/*
		 * Errors from the S3 service
		 */
		case S3StatusErrorAccessDenied:
		case S3StatusErrorAccountProblem:
		case S3StatusErrorAmbiguousGrantByEmailAddress:
		case S3StatusErrorBadDigest:
			rc = EACCES;
			break;

		case S3StatusErrorBucketAlreadyExists:
			rc = EEXIST;
			break;

		case S3StatusErrorBucketAlreadyOwnedByYou:
		case S3StatusErrorBucketNotEmpty:
			rc = ENOTEMPTY;
			break;

		case S3StatusErrorCredentialsNotSupported:
		case S3StatusErrorCrossLocationLoggingProhibited:
			rc = EACCES;
			break;

		case S3StatusErrorEntityTooSmall:
		case S3StatusErrorEntityTooLarge:
			rc = EINVAL;
			break;

		case S3StatusErrorExpiredToken:
			rc = EKEYEXPIRED;
			break;

		case S3StatusErrorIllegalVersioningConfigurationException:
		case S3StatusErrorIncompleteBody:
		case S3StatusErrorIncorrectNumberOfFilesInPostRequest:
			rc = EINVAL;
			break;

		case S3StatusErrorInlineDataTooLarge:
		case S3StatusErrorInternalError:
		case S3StatusErrorInvalidAccessKeyId:
			rc = EACCES;
			break;

		case S3StatusErrorInvalidAddressingHeader:
		case S3StatusErrorInvalidArgument:
		case S3StatusErrorInvalidBucketName:
		case S3StatusErrorInvalidBucketState:
		case S3StatusErrorInvalidDigest:
		case S3StatusErrorInvalidEncryptionAlgorithmError:
		case S3StatusErrorInvalidLocationConstraint:
		case S3StatusErrorInvalidObjectState:
		case S3StatusErrorInvalidPart:
		case S3StatusErrorInvalidPartOrder:
		case S3StatusErrorInvalidPayer:
		case S3StatusErrorInvalidPolicyDocument:
		case S3StatusErrorInvalidRange:
		case S3StatusErrorInvalidRequest:
		case S3StatusErrorInvalidSecurity:
		case S3StatusErrorInvalidSOAPRequest:
		case S3StatusErrorInvalidStorageClass:
		case S3StatusErrorInvalidTargetBucketForLogging:
		case S3StatusErrorInvalidToken:
		case S3StatusErrorInvalidURI:
			rc = EINVAL;
			break;

		case S3StatusErrorKeyTooLong:
			rc = ENAMETOOLONG;
			break;

		case S3StatusErrorMalformedACLError:
		case S3StatusErrorMalformedPOSTRequest:
		case S3StatusErrorMalformedXML:
		case S3StatusErrorMaxMessageLengthExceeded:
		case S3StatusErrorMaxPostPreDataLengthExceededError:
		case S3StatusErrorMetadataTooLarge:
		case S3StatusErrorMethodNotAllowed:
		case S3StatusErrorMissingAttachment:
		case S3StatusErrorMissingContentLength:
		case S3StatusErrorMissingRequestBodyError:
		case S3StatusErrorMissingSecurityElement:
		case S3StatusErrorMissingSecurityHeader:
		case S3StatusErrorNoLoggingStatusForKey:
			rc = EINVAL;
			break;

		case S3StatusErrorNoSuchBucket:
		case S3StatusErrorNoSuchKey:
			rc = ENOKEY;
			break;

		case S3StatusErrorNoSuchLifecycleConfiguration:
		case S3StatusErrorNoSuchUpload:
		case S3StatusErrorNoSuchVersion:
			rc = EEXIST;
			break;

		case S3StatusErrorNotImplemented:
			rc = EPERM;
			break;

		case S3StatusErrorNotSignedUp:
			rc = EACCES;
			break;

		case S3StatusErrorNoSuchBucketPolicy:
			rc = ENOKEY;
			break;

		case S3StatusErrorOperationAborted:
		case S3StatusErrorPermanentRedirect:
		case S3StatusErrorPreconditionFailed:
		case S3StatusErrorRedirect:
		case S3StatusErrorRestoreAlreadyInProgress:
		case S3StatusErrorRequestIsNotMultiPartContent:
			rc = ECONNABORTED;
			break;

		case S3StatusErrorRequestTimeout:
			rc = ETIMEDOUT;
			break;

		case S3StatusErrorRequestTimeTooSkewed:
		case S3StatusErrorRequestTorrentOfBucketError:
		case S3StatusErrorSignatureDoesNotMatch:
		case S3StatusErrorServiceUnavailable:
		case S3StatusErrorSlowDown:
		case S3StatusErrorTemporaryRedirect:
		case S3StatusErrorTokenRefreshRequired:
		case S3StatusErrorTooManyBuckets:
		case S3StatusErrorUnexpectedContent:
		case S3StatusErrorUnresolvableGrantByEmailAddress:
		case S3StatusErrorUserKeyMustBeSpecified:
		case S3StatusErrorQuotaExceeded:
		case S3StatusErrorUnknown:
			rc = ECONNABORTED;
			break;

		/*
		* The following are HTTP errors returned by S3 without enough
		* detail to distinguish any of the above S3StatusError
		* conditions
		*/
		case S3StatusHttpErrorMovedTemporarily:
		case S3StatusHttpErrorBadRequest:
		case S3StatusHttpErrorForbidden:
		case S3StatusHttpErrorNotFound:
		case S3StatusHttpErrorConflict:
		case S3StatusHttpErrorUnknown:
			rc = ECONNABORTED;
			break;

	}
	return rc;
}
