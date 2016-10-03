package com.tencent.jstorm.ql.parse;

public class ASTNodeOrigin {
  private final String objectType;
  private final String objectName;
  private final String objectDefinition;
  private final String usageAlias;
  private final ASTNode usageNode;

  public ASTNodeOrigin(String objectType, String objectName,
      String objectDefinition, String usageAlias, ASTNode usageNode) {
    this.objectType = objectType;
    this.objectName = objectName;
    this.objectDefinition = objectDefinition;
    this.usageAlias = usageAlias;
    this.usageNode = usageNode;
  }

  /**
   * @return the type of the object from which an ASTNode originated, e.g.
   *         "view".
   */
  public String getObjectType() {
    return objectType;
  }

  /**
   * @return the name of the object from which an ASTNode originated, e.g. "v".
   */
  public String getObjectName() {
    return objectName;
  }

  /**
   * @return the definition of the object from which an ASTNode originated, e.g.
   *         <code>select x+1 as y from t</code>.
   */
  public String getObjectDefinition() {
    return objectDefinition;
  }

  /**
   * @return the alias of the object from which an ASTNode originated, e.g. "v1"
   *         (this can help with debugging context-dependent expansions)
   */
  public String getUsageAlias() {
    return usageAlias;
  }

  /**
   * @return the expression node triggering usage of an object from which an
   *         ASTNode originated, e.g. <code>v as v1</code> (this can help with
   *         debugging context-dependent expansions)
   */
  public ASTNode getUsageNode() {
    return usageNode;
  }

}
