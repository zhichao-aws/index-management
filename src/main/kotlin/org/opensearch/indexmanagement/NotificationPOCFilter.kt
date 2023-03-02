/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement

import org.apache.logging.log4j.LogManager
import org.opensearch.action.ActionListener
import org.opensearch.action.ActionRequest
import org.opensearch.action.ActionResponse
import org.opensearch.action.admin.indices.forcemerge.ForceMergeAction
import org.opensearch.action.admin.indices.shrink.ResizeAction
import org.opensearch.action.admin.indices.shrink.ResizeRequest
import org.opensearch.action.support.ActionFilter
import org.opensearch.action.support.ActionFilterChain
import org.opensearch.action.support.ActiveShardsObserver
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.settings.Settings
import org.opensearch.common.unit.TimeValue
import org.opensearch.index.reindex.ReindexAction
import org.opensearch.tasks.Task
import org.opensearch.threadpool.ThreadPool
import java.util.concurrent.TimeUnit

private val log = LogManager.getLogger(NotificationPOCFilter::class.java)

class NotificationPOCFilter(
    val clusterService: ClusterService,
    val settings: Settings,
    val threadpool: ThreadPool
) : ActionFilter {

    val activeShardsObserver = ActiveShardsObserver(clusterService, threadpool)

    override fun order(): Int {
        return Integer.MAX_VALUE
    }

    override fun <Request : ActionRequest?, Response : ActionResponse?> apply(
        task: Task,
        action: String,
        request: Request,
        listener: ActionListener<Response>,
        chain: ActionFilterChain<Request, Response>
    ) {
        if (!TARGET_ACTIONS.contains(action) || null != request!!.parentTask) {
            chain.proceed(task, action, request, listener)
        } else {
            chain.proceed(
                task,
                action,
                request,
                object : ActionListener<Response> {
                    override fun onResponse(response: Response) {
                        if (!RECOVERY_ACTIONS.contains(action)) {
                            sendNotification(action, "success")
                        } else {
                            request as ResizeRequest
                            activeShardsObserver.waitForActiveShards(
                                arrayOf<String>(request.targetIndexRequest.index()),
                                request.targetIndexRequest.waitForActiveShards(),
                                TimeValue(30, TimeUnit.MINUTES),
                                { shardsAcknowledged: Boolean ->
                                    if (shardsAcknowledged) {
                                        sendNotification(action, "success")
                                    } else {
                                        sendNotification(action, "timeout")
                                    }
                                },
                            ) { e: Exception? ->
                                sendNotification(action, "failed ${e?.message}")
                            }
                        }
                        listener.onResponse(response)
                    }

                    override fun onFailure(e: Exception?) {
                        sendNotification(action, "failed ${e?.message}")
                        listener.onFailure(e)
                    }
                }
            )
        }
    }

    fun sendNotification(actionName: String, state: String) {
        val message = "$actionName is $state"
//        just poc here
        log.warn(message)
    }

    companion object {
        val TARGET_ACTIONS = setOf<String>(ReindexAction.NAME, ResizeAction.NAME, ForceMergeAction.NAME)
        val RECOVERY_ACTIONS = setOf<String>(ResizeAction.NAME)
    }
}
