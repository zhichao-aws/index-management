/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.adminpanel.notification.filter

import org.apache.logging.log4j.LogManager
import org.opensearch.action.ActionListener
import org.opensearch.action.ActionRequest
import org.opensearch.action.ActionResponse
import org.opensearch.action.admin.indices.forcemerge.ForceMergeAction
import org.opensearch.action.admin.indices.open.OpenIndexAction
import org.opensearch.action.admin.indices.shrink.ResizeAction
import org.opensearch.action.support.ActionFilter
import org.opensearch.action.support.ActionFilterChain
import org.opensearch.action.support.ActiveShardsObserver
import org.opensearch.client.Client
import org.opensearch.cluster.metadata.IndexNameExpressionResolver
import org.opensearch.cluster.service.ClusterService
import org.opensearch.index.reindex.ReindexAction
import org.opensearch.script.ScriptService
import org.opensearch.tasks.Task
import org.opensearch.tasks.TaskId

class IndexOperationActionFilter(
    val client: Client,
    val clusterService: ClusterService,
    val scriptService: ScriptService,
    val activeShardsObserver: ActiveShardsObserver,
    val indexNameExpressionResolver: IndexNameExpressionResolver
) : ActionFilter {

    private val logger = LogManager.getLogger(IndexOperationActionFilter::class.java)

    override fun order() = Integer.MIN_VALUE
    override fun <Request : ActionRequest, Response : ActionResponse> apply(
        task: Task,
        action: String,
        request: Request,
        listener: ActionListener<Response>,
        chain: ActionFilterChain<Request, Response>
    ) {
        chain.proceed(task, action, request, wrapActionListener(task, action, request, listener))
    }

    fun <Request : ActionRequest, Response : ActionResponse> wrapActionListener(
        task: Task,
        action: String,
        request: Request,
        listener: ActionListener<Response>,
    ): ActionListener<Response> {
        var wrappedListener = listener
        when (action) {
            ReindexAction.NAME,
            OpenIndexAction.NAME,
            ResizeAction.NAME,
            ForceMergeAction.NAME -> {
                if (task.parentTaskId.isSet == false) {
                    val taskId = TaskId(clusterService.localNode().id, task.id)
                    logger.info("Add notification action listener for tasks: {} and action: {} ", taskId.toString(), action)
                    wrappedListener = NotificationActionListener(
                        delegate = listener,
                        client = client,
                        action = action,
                        clusterService = clusterService,
                        task = task,
                        scriptService = scriptService,
                        request = request,
                        activeShardsObserver = activeShardsObserver,
                        indexNameExpressionResolver = indexNameExpressionResolver
                    )
                }
            }
        }
        return wrappedListener
    }
}