/* Copyright 2010-2014 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.boundary.aws.sqs;

import java.util.Date;
import java.util.List;
import java.util.Map.Entry;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.DeleteQueueRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;

/**
 * This sample demonstrates how to make basic requests to Amazon SQS using the
 * AWS SDK for Java.
 * <p>
 * <b>Prerequisites:</b> You must have a valid Amazon Web Services developer
 * account, and be signed up to use Amazon SQS. For more information on Amazon
 * SQS, see http://aws.amazon.com/sqs.
 * <p>
 * WANRNING:</b> To avoid accidental leakage of your credentials, DO NOT keep
 * the credentials file in your source directory.
 */
public class Sample {

	public static void main(String[] args) throws Exception {
		String queueName = "boundary-sqs-demo-queue";

		/*
		 * The ProfileCredentialsProvider will return your [default] credential
		 * profile by reading from the credentials file located at
		 * (HOME/.aws/credentials).
		 */
		AWSCredentials credentials = null;
		try {
			credentials = new ProfileCredentialsProvider("default")
					.getCredentials();
		} catch (Exception e) {
			throw new AmazonClientException("Cannot load the credentials from the credential profiles file. ",e);
		}

		AmazonSQS sqs = new AmazonSQSClient(credentials);
		Region usWest2 = Region.getRegion(Regions.US_WEST_2);
		sqs.setRegion(usWest2);

		try {
			// Create a queue
			System.out.printf("Creating queue: %s.\n",queueName);
			CreateQueueRequest createQueueRequest = new CreateQueueRequest(queueName);
			String myQueueUrl = sqs.createQueue(createQueueRequest).getQueueUrl();

			int messageCount = 100;

			// Send a messages
			for (int count = 1; count <= messageCount ; count++) {
				System.out.printf("Sending message %3d to %s.\n",count, queueName);
				sqs.sendMessage(new SendMessageRequest(myQueueUrl, new Date() + ": This is my message text."));
			}

			for (int count = 1; count <= messageCount ; count++) {

				ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(myQueueUrl);
				List<Message> messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
				for (Message msg : messages) {
					System.out.printf("Received message: %s queue: %s body: %s\n",
							msg.getMessageId(),queueName,msg.getBody());
					System.out.printf("Deleting message: %s queue: %s\n",msg.getMessageId(), queueName);
					String messageRecieptHandle = msg.getReceiptHandle();
					sqs.deleteMessage(new DeleteMessageRequest(myQueueUrl,messageRecieptHandle));
				}
			}

			System.out.printf("Deleting queue: %s\n",queueName);
			sqs.deleteQueue(new DeleteQueueRequest(myQueueUrl));
			
		} catch (AmazonServiceException ase) {
			System.out
					.println("Caught an AmazonServiceException, which means your request made it "
							+ "to Amazon SQS, but was rejected with an error response for some reason.");
			System.out.println("Error Message:    " + ase.getMessage());
			System.out.println("HTTP Status Code: " + ase.getStatusCode());
			System.out.println("AWS Error Code:   " + ase.getErrorCode());
			System.out.println("Error Type:       " + ase.getErrorType());
			System.out.println("Request ID:       " + ase.getRequestId());
		} catch (AmazonClientException ace) {
			System.out
					.println("Caught an AmazonClientException, which means the client encountered "
							+ "a serious internal problem while trying to communicate with SQS, such as not "
							+ "being able to access the network.");
			System.out.println("Error Message: " + ace.getMessage());
		}
		
		sqs.shutdown();
	}
}
