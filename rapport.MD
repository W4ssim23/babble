# Lab 6: Babble

## Participants

- Mai Duc An
- Zouiten Ouassim

## Platforms

- For Zouiten Ouassim:

  OS: Ubuntu 24.04.2 LTS on Windows 10 x86_64  
  Kernel: 6.6.87.2-microsoft-standard-WSL2  
  CPU architecture: x86_64

- For Mai Duc An:

  OS: Ubuntu 26.04.1 on UTM  
  Kernel: 7.0.0-38-generic  
  CPU architecture: aarch64

## Stage 1

At this stage, we tried to solve both of the problems in the way that the questions required and the figures showed.

For the registration table, we solved the reader-writer problem (inside the functions used to interact with the structure), and gave the writer more priority than the readers, and that was because of the assumption we made: "We write to the structure on logins and unregistering, and we read from it in the rest of the commands. Logic says that a social media app would most likely have more posts published and read than new users creating accounts (or deleting accounts), even with the 'never remember again' policy of the server, so it might be better to give the commands that are less frequent a better chance so they don't starve." The implementation was with locks and condition variables (same as the one in the TD, just the priority is different).

For the producer-consumer problem, and to keep the code clean, we created a new file, 'babble_threads.c', that contains the core of the execution threads and the communication threads (so we had to remove `static` from some functions in babble_server.c because they will be called from that file). We added a buffer for the received commands that gets initialized before generating the threads (a new struct, `cmd_buff_t`), that stores the commands and keeps track of the next one to execute (FIFO). We focused on respecting the requested architecture by creating the cores, and we minimized the critical sections as much as possible (or at least what we think is the limit), and the solution was implemented using semaphores (pretty similar to the course).

## Stage 2

#### a- Analyzing the first commands

For the publish command, we first get the client's bundle, then add the message to all the followers' timelines (and detect if some of them have disconnected). After that, we clean the followers list and create an answer if it's expected.

For the follow command, we get the client bundle of the follower and the client bundle of the one being followed, then check if the first client already follows the second one, and add him if he doesn't (inside the bundle of the one being followed). After that, we generate an answer if it's expected.

For the timeline command, we get the client's bundle and generate the timeline summary, which involves reading and writing to the timeline stored in the bundle.

If there are multiple executors, we might have multiple problems. The timeline might get written to and read from at the same time, so we have a read-write problem between timeline insert and generate (insert/insert, insert/generate, generate/generate). We also have the followers list, which is read and modified by publish and modified by follow.

For example, two publications from the same user at once might cause problems, getting the timeline while a client we follow is publishing might also cause problems, and the same goes for follow while publish is executing.

So we concluded that the structures we need to protect are the timeline and the followers list.

#### b- Analyzing the RDV command

For the RDV command, the problem is that it might actually send a response before all the previous commands have been executed. Since we are now executing commands concurrently, the last command getting served is no longer a guarantee that all the previous ones have finished executing.

#### c- Implementation

#### Implementing the first part

This took multiple steps to figure out. We didn't want to block all the executors based on the command type. If we did so, the benefit from multiple execution threads would have been so minimal. We wanted to wait only for the structure we will manipulate that actually belongs to the current client and is currently in use by another thread. To get this distinction, for the timeline structure, we added a lock to it, and made the two functions wait based on that lock (both of them read and write, so it's not a reader-writer problem). And for the followers manipulation, since followers are stored in the client bundle directly, we added a mutex to the bundle to tell if the followers are being modified for this specific client. We placed the locks around only the parts where the followers are being manipulated.

We didn't have to worry about other data races like the `disconnected` attribute. Reading it as 0 just when it's getting changed to 1 by unregister won't actually matter that much, because it will get wiped anyway if the command gets called again later on.

#### Implementing the second part

For the RDV command, we first thought about adding a counter to the client bundle to keep track of the commands that are currently being executed, and using a mutex and a condition variable to make RDV wait until this counter reaches 0. But we realized that this wouldn't work if we incremented the counter inside the execution functions, because RDV might get the lock and check the counter before some of the previous commands have had the chance to increment it, and it would respond even though those commands haven't finished yet.

To solve this problem, we decided to increment the counter in the communication thread instead, just before adding the command to the shared buffer. Since each client has its own communication thread, the commands are received in order, so we can make sure that all the previous commands have already been counted before RDV gets executed.

For the decrement, we did it directly inside the functions that execute the commands (publish, follow, timeline, and follow count), after they finish their work. We protected the counter using a mutex, and whenever it reaches 0, we wake up the thread waiting on the condition variable.

For RDV itself, we don't increment the counter, otherwise it would end up waiting for itself. Instead, when it gets executed, it gets the client's bundle, locks the counter mutex, and waits on the condition variable while the counter is different from 0. Once all the previous commands have finished and the counter reaches 0, RDV can continue and generate its answer.

We also made two assumptions for this solution. The first one is that the client will wait for the RDV response before sending any new commands, so we don't have to worry about commands arriving after RDV and increasing the counter while it's waiting. The second one is that the shared command buffer follows FIFO, so when an execution thread gets RDV, all the commands before it have already been taken by other execution threads or have finished executing. This means that RDV won't be waiting for a command that is still in the buffer while all the executors are blocked waiting for RDV.

This way, we can make sure that RDV only responds when all the previous commands from the same client have finished executing, without having to block commands from other clients.

#### Testing

To test our implementation, we first tried testing it manually by launching the server and multiple clients, and executing different commands to check if everything was working as expected, especially when multiple clients were interacting with each other at the same time. After that, we used the automatic tests provided with the lab to test the implementation under more complex situations that would be difficult to reproduce manually. We also tried changing the configuration multiple times, such as the number of execution threads, the size of the command buffer, and the maximum delay, and running the tests with and without streaming and random delays.

And after multiple refinements, each time we discovered that there was a bug, we got the final result in the Stage 2 folder. It didn't fail any test we tried it on, and we made sure to run the automated ones as many times as possible, so we reduced the chance that we were just lucky.

We also noticed that the test execution times got much lower when using a large number of clients in `stress_test` with `-r -s` enabled and multiple execution threads. This was especially noticeable compared to using only one execution thread, since multiple commands could now be executed concurrently instead of waiting for a single executor to process them one by one.

## Stage 0 Questions

1. **Listening socket:** The server starts by calling `server_connection_init(portno)` from `main()` (in babble_server_implem.c). This function creates a TCP socket using `socket()`, binds it to the requested port using `bind()`, and starts listening for incoming connections using `listen()`.

2. **New connections:** When a new client tries to connect, `server_connection_accept(sockfd)` calls `accept()` and returns the socket associated with that client. In Stage 1, `main()` creates a new communication thread for each accepted connection.

3. **Message processing:** The server first receives a message using `network_recv()`, then parses it into a `command_t` using `parse_command()`. After that, the command gets executed through `process_command()`, which calls the corresponding `run_*_command()` function. If an answer is expected, it gets sent using `send_answer_to_client()`, and the temporary memory is freed afterward. In Stage 1, the commands first go through the shared buffer before being executed by the execution threads.

4. **Special LOGIN handling:** Before LOGIN, the client isn't registered yet and doesn't have a key. The `run_login_command()` function handles the registration, computes the client's key, initializes the client's data, and associates the socket with that client. That's why LOGIN has to be processed first, before the other commands can use the client's key.

5. **Registration table:** The `registration_table` stores pointers to the registered clients' `client_bundle_t` structures. These structures contain information such as the client's key, name, socket, followers, and timeline. The table allows us to insert new clients, look up existing ones, and remove them when needed.

6. **Keys:** During LOGIN, a key is generated from the client's name using `hash()`. This key is used to identify the client when processing commands, looking up clients in the registration table, generating answers, and sending responses to the correct socket. For FOLLOW, we also hash the name of the client being followed to find their bundle.

7. **Answers:** An `answer_t` structure contains the key of the client receiving the answer, the number of messages, and a linked list containing those messages. The command functions create answers using `alloc_answer()` and add messages using `add_msg_to_answer()`. After that, `send_answer_to_client()` sends the number of messages followed by each message using `write_to_client()`, and `free_answer()` frees the allocated memory.

8. **FOLLOW:** The `run_follow_command()` function first gets the bundle of the client requesting to follow someone, then finds the other client's bundle by hashing their name. If that client exists and isn't already being followed, we add the requester to the target client's `followers[]` list and increment `nb_followers`. An answer is also generated if it's expected.

9. **PUBLISH:** The `run_publish_command()` function first gets the publisher's bundle, then inserts the publication into the timeline of each connected follower (including the publisher, since each client follows themselves). It also detects and removes disconnected followers from the list, and generates an answer if it's expected.

10. **TIMELINE:** The `run_timeline_command()` function first gets the requesting client's bundle, then calls `timeline_generate_summary()` to generate an answer containing the client's timeline.
