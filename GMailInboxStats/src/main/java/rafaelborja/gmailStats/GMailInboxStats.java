package rafaelborja.gmailStats;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.extensions.java6.auth.oauth2.AuthorizationCodeInstalledApp;
import com.google.api.client.extensions.jetty.auth.oauth2.LocalServerReceiver;
import com.google.api.client.googleapis.auth.oauth2.GoogleAuthorizationCodeFlow;
import com.google.api.client.googleapis.auth.oauth2.GoogleClientSecrets;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.util.store.FileDataStoreFactory;
import com.google.api.services.gmail.Gmail;
import com.google.api.services.gmail.GmailScopes;
import com.google.api.services.gmail.model.Label;
import com.google.api.services.gmail.model.ListLabelsResponse;
import com.google.api.services.gmail.model.ListMessagesResponse;
import com.google.api.services.gmail.model.Message;

/**
 * Simple app to read your gmail inbox and provide status about the messages
 * waiting to be cleaned.
 * 
 * This version shows the number of messages from each recipient based on the user query.
 * 
 * @see #QUERRY
 * @see #USER
 * 
 * https://github.com/rafaelborja/GmailInboxStats
 * 
 * @author Rafael Borja
 *
 */
public class GMailInboxStats {
	/** Application name. */
	private static final String APPLICATION_NAME = "Gmail API Java Quickstart";

	/** Directory to store user credentials for this application. */
	private static final java.io.File DATA_STORE_DIR = new java.io.File(System.getProperty("user.home"),
			".credentials/gmail-java-quickstart");

	/** Global instance of the {@link FileDataStoreFactory}. */
	private static FileDataStoreFactory DATA_STORE_FACTORY;

	/** Global instance of the JSON factory. */
	private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();

	/** Global instance of the HTTP transport. */
	private static HttpTransport HTTP_TRANSPORT;

	/**
	 * Global instance of the scopes required by this quickstart.
	 *
	 * If modifying these scopes, delete your previously saved credentials at
	 * ~/.credentials/gmail-java-quickstart
	 */
	private static final List<String> SCOPES = Arrays.asList(GmailScopes.GMAIL_LABELS, GmailScopes.GMAIL_READONLY);

	/** Pattern to capture valid email addresses */
	public static final Pattern VALID_EMAIL_ADDRESS_REGEX = Pattern
			.compile("(?<email>[A-Z0-9._%+-]+@[A-Z0-9.-]+\\.[A-Z]{2,6})", Pattern.CASE_INSENSITIVE);

	/** Number of threads to simultaneous download message headers */
	final static String PARALLEL_THREADS = "32";

	/**
	 * Minimum number of messages from the same recipient to be shown in results
	 */
	final static int MIN_OCURRENCES_THRESHOLD = 5;
	
	/** User query. Scope of stats */
	final static String QUERRY = "label:INBOX";
	
	final static String USER = "rafaelborja@gmail.com";

	static {
		try {
			HTTP_TRANSPORT = GoogleNetHttpTransport.newTrustedTransport();
			DATA_STORE_FACTORY = new FileDataStoreFactory(DATA_STORE_DIR);
		} catch (Throwable t) {
			t.printStackTrace();
			System.exit(1);
		}
	}

	/**
	 * Creates an authorized Credential object.
	 * 
	 * @return an authorized Credential object.
	 * @throws IOException
	 */
	public static Credential authorize() throws IOException {
		// Load client secrets.
		InputStream in = GMailInboxStats.class.getResourceAsStream("/client_secret.json");
		GoogleClientSecrets clientSecrets = GoogleClientSecrets.load(JSON_FACTORY, new InputStreamReader(in));

		// Build flow and trigger user authorization request.
		GoogleAuthorizationCodeFlow flow = new GoogleAuthorizationCodeFlow.Builder(HTTP_TRANSPORT, JSON_FACTORY,
				clientSecrets, SCOPES).setDataStoreFactory(DATA_STORE_FACTORY).setAccessType("offline").build();
		Credential credential = new AuthorizationCodeInstalledApp(flow, new LocalServerReceiver()).authorize("user");
		System.out.println("Credentials saved to " + DATA_STORE_DIR.getAbsolutePath());
		return credential;
	}

	/**
	 * Build and return an authorized Gmail client service.
	 * 
	 * @return an authorized Gmail client service
	 * @throws IOException
	 */
	public static Gmail getGmailService() throws IOException {
		Credential credential = authorize();
		return new Gmail.Builder(HTTP_TRANSPORT, JSON_FACTORY, credential).setApplicationName(APPLICATION_NAME).build();
	}

	public static void main(String[] args) throws Exception {
		// Build a new authorized API client service.
		Gmail service = getGmailService();

		// Print the labels in the user's account.
		ListLabelsResponse listResponse = service.users().labels().list(USER).execute();
		List<Label> labels = listResponse.getLabels();
		if (labels.size() == 0) {
			System.out.println("No labels found.");
		} else {
			System.out.println("Labels:");
			for (Label label : labels) {
				System.out.printf("- %s\n", label.getName());
			}
		}
		
		ListMessagesResponse response = service.users().messages().list(USER).setQ(QUERRY).execute();
		// label:unread
		List<Message> messages = new ArrayList<Message>();
		System.out.println("Query " + QUERRY + " Result size: " + response.getResultSizeEstimate());
		while (response.getMessages() != null) {
			messages.addAll(response.getMessages());
			if (response.getNextPageToken() != null) {
				String pageToken = response.getNextPageToken();
				response = service.users().messages().list(USER).setQ(QUERRY).setPageToken(pageToken).execute(); //
			} else {
				break;
			}
		}

		// Stores number of occurrences for a single email addresses
		Map<String, Integer> emailAddressCountMap = new HashMap<String, Integer>();

		// Changing number of concurrent threads in a parallel stream
		System.setProperty("java.util.concurrent.ForkJoinPool.common.parallelism", PARALLEL_THREADS);

		// Iterate over all messages
		messages.parallelStream().forEach(message -> {
			try {
				// Retrieves header only, not full message
				Message m1 = service.users().messages().get(USER, message.getId()).setFields("payload/headers")
						.execute();

				// Extract FROM email address
				Stream<String> fromHeaderValue = m1.getPayload().getHeaders().stream()
						.filter(h -> "From".equals(h.getName())).map(h -> getEmailAddress(h.getValue()));

				// Stores email address in address/count map
				String[] tmpArray = fromHeaderValue.toArray(String[]::new);
				if (tmpArray.length > 0) {
					String emailAddress = tmpArray[0];
					synchronized (GMailInboxStats.class) {
						Integer count = emailAddressCountMap.get(emailAddress);
						if (count == null) {
							count = 0;
						}
						emailAddressCountMap.put(emailAddress, count + 1);

						System.out.println(emailAddress + ": " + count);
					}
				}
			}

			catch (Exception e) {
				e.printStackTrace();
			}
		});

		
		// FILTER AND DISPLAY RESULTS
		Stream<Entry<String, Integer>> sortedEmailAddressCountMap = emailAddressCountMap.entrySet().stream()
				.filter(e -> {
					return e.getValue() > MIN_OCURRENCES_THRESHOLD;
				}).sorted((e1, e2) -> e1.getValue().compareTo(e2.getValue()));

		System.out.println("RESULTS ORDERED AND FILTERED");
		sortedEmailAddressCountMap.forEach(e -> {
			System.out.println("Address : " + e.getKey() + " Count : " + e.getValue());
		});

	}

	/**
	 * Extract the email address contained in the string
	 * 
	 * @return email address or original string if no valid email is found.
	 */
	public static String getEmailAddress(String emailField) {
		Matcher emailMatcher = VALID_EMAIL_ADDRESS_REGEX.matcher(emailField);
		if (emailMatcher.find()) {
			return emailMatcher.group("email").toLowerCase();
		} else
			return emailField;
	}

}